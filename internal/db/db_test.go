package db

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/talaria0101/moraine/internal/key"
	"github.com/talaria0101/moraine/internal/manifest"
)

func openTest(t *testing.T, opts Options) (*DB, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	d, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return d, dir
}

type modelVal struct {
	val []byte
	del bool
}

func smallOpts() Options {
	return Options{
		WriteBuffer:         48 << 10,
		TargetFileSize:      24 << 10,
		L0CompactionTrigger: 2,
		L1TargetBytes:       64 << 10,
		MaxLevels:           5,
	}
}

// stepEngine forces a memtable rotation, waits for the flush queue to
// drain, then waits for background compactions to go quiet.
func stepEngine(t *testing.T, db *DB) {
	t.Helper()
	db.mu.Lock()
	empty := db.mem.Size() == 0
	if !empty {
		db.rotateLocked()
	}
	db.mu.Unlock()
	want := db.seq.Load()
	if empty {
		// Nothing new to flush; any earlier flush already published
		// or will, and flushedTo covers it.
		want = db.flushedTo.Load()
	}
	for i := 0; i < 10000; i++ {
		db.mu.Lock()
		n := len(db.imm)
		db.mu.Unlock()
		// The flush loop pops its job before flushing, so an empty
		// queue does not mean the data is published yet.
		if n == 0 && db.flushedTo.Load() >= want {
			break
		}
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 10000; i++ {
		if db.pickCompaction() == nil && len(db.imm) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("engine never went quiet")
}

func checkGet(t *testing.T, db *DB, k string, want modelVal, phase string) {
	t.Helper()
	got, found, err := db.Get([]byte(k))
	if err != nil {
		t.Fatalf("%s: get %s: %v", phase, k, err)
	}
	if want.val == nil && !want.del {
		// Zero modelVal: the key was never written, so absence is
		// expected. A real put of an empty value carries a non-nil
		// empty slice and does not take this branch.
		if found {
			t.Fatalf("%s: get %s: found %q, want absent", phase, k, got)
		}
		return
	}
	if want.del {
		if found {
			t.Fatalf("%s: get %s: found, want deleted", phase, k)
		}
		return
	}
	if !found {
		t.Fatalf("%s: get %s: not found, want %q", phase, k, want.val)
	}
	if string(got) != string(want.val) {
		t.Fatalf("%s: get %s: got %q want %q", phase, k, got, want.val)
	}
}

func TestDBModelLeveled(t *testing.T) { runModelTest(t, smallOpts()) }

// TestPickCompactionShallowTie locks the picker's tie-break: at equal
// score the shallowest level wins. The deepest-on-tie variant read as
// a one-token bug (>= instead of >) and inverted the tiered/lazy WAF
// ordering in the benchmark, so this behavior is load-bearing.
func TestPickCompactionShallowTie(t *testing.T) {
	d, _ := openTest(t, Options{TieredFiles: 4, MaxLevels: 5, Compaction: Tiered})
	defer d.Close()

	ik := func(user string) []byte { return key.Encode(nil, []byte(user), 1, key.TypeSet) }
	meta := func(level int, user string, size uint64) manifest.TableMeta {
		return manifest.TableMeta{ID: d.allocFileID(), Level: level, Size: size,
			Smallest: ik(user + "a"), Largest: ik(user + "z")}
	}

	// L1 and L2 both at exactly TieredFiles files: scores equal, so
	// the shallow level must be picked.
	v := &manifest.Version{Levels: make([][]manifest.TableMeta, 3)}
	for _, user := range []string{"a", "m", "x", "z"} {
		v.Levels[1] = append(v.Levels[1], meta(1, user, 100))
	}
	for _, user := range []string{"b", "n", "y", "q"} {
		v.Levels[2] = append(v.Levels[2], meta(2, user, 100))
	}
	c := d.pickFrom(v)
	if c == nil || c.level != 1 {
		t.Fatalf("tie at score 1.0: picked %+v, want level 1", c)
	}

	// A deeper level strictly over budget outranks a shallow one at
	// exactly budget: highest score wins.
	for i := 0; i < 4; i++ {
		v.Levels[2] = append(v.Levels[2], meta(2, "w", 100))
	}
	c = d.pickFrom(v)
	if c == nil || c.level != 2 {
		t.Fatalf("L2 score 2.0 vs L1 score 1.0: picked %+v, want level 2", c)
	}

	// Nothing at or above 1.0: no compaction.
	empty := &manifest.Version{Levels: make([][]manifest.TableMeta, 2)}
	empty.Levels[1] = []manifest.TableMeta{meta(1, "a", 100)}
	if c := d.pickFrom(empty); c != nil {
		t.Fatalf("under-budget tree picked %+v, want nil", c)
	}
}

func TestDBModelTiered(t *testing.T) {
	o := smallOpts()
	o.Compaction = Tiered
	o.TieredFiles = 2
	runModelTest(t, o)
}

func TestDBModelLazy(t *testing.T) {
	o := smallOpts()
	o.Compaction = LazyLeveled
	o.TieredFiles = 2
	runModelTest(t, o)
}

func runModelTest(t *testing.T, opts Options) {
	db, _ := openTest(t, opts)
	defer db.Close()
	rng := rand.New(rand.NewSource(7))
	model := map[string]modelVal{}

	for step := 0; step < 4000; step++ {
		k := fmt.Sprintf("k%04d", rng.Intn(250))
		switch n := rng.Intn(100); {
		case n < 55:
			v := make([]byte, rng.Intn(120))
			rng.Read(v)
			if err := db.Put([]byte(k), v); err != nil {
				t.Fatalf("put: %v", err)
			}
			model[k] = modelVal{val: v}
		case n < 75:
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatalf("delete: %v", err)
			}
			model[k] = modelVal{del: true}
		default:
			checkGet(t, db, k, model[k], "model")
		}
		if step%137 == 0 {
			stepEngine(t, db)
			for i := 0; i < 20; i++ {
				k := fmt.Sprintf("k%04d", rng.Intn(250))
				checkGet(t, db, k, model[k], "post-step")
			}
		}
	}
	stepEngine(t, db)

	// Full scan must match the model exactly.
	it, err := db.NewIter()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	keys := make([]string, 0, len(model))
	for k, mv := range model {
		if !mv.del {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	i := 0
	for it.First(); it.Valid(); it.Next() {
		if i >= len(keys) {
			t.Fatalf("scan yielded more than %d keys", len(keys))
		}
		if string(it.UserKey()) != keys[i] {
			t.Fatalf("scan key %q, want %q (i=%d)", it.UserKey(), keys[i], i)
		}
		if string(it.Value()) != string(model[keys[i]].val) {
			t.Fatalf("scan value mismatch at %s", keys[i])
		}
		i++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if i != len(keys) {
		t.Fatalf("scan yielded %d keys, want %d", i, len(keys))
	}

	// Every level deeper than L0 must hold disjoint user ranges.
	v := db.man.Current()
	for l := 1; l < len(v.Levels); l++ {
		prevMax := []byte(nil)
		for _, m := range v.Levels[l] {
			lo := userOf(m.Smallest)
			hi := userOf(m.Largest)
			if prevMax != nil && key.CompareUser(prevMax, lo) >= 0 {
				t.Fatalf("level %d ranges overlap: %q <= %q", l, prevMax, lo)
			}
			prevMax = hi
		}
	}
}

func TestDBRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	model := map[string]modelVal{}
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("key%04d", i)
		if i%7 == 3 {
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
			model[k] = modelVal{del: true}
			continue
		}
		v := []byte(fmt.Sprintf("value-%d", i))
		if err := db.Put([]byte(k), v); err != nil {
			t.Fatal(err)
		}
		model[k] = modelVal{val: v}
	}
	// Simulate a crash: abandon the handle without Close. The WAL
	// writes are already in the OS page cache and must replay.
	_ = db

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	for k, mv := range model {
		checkGet(t, db2, k, mv, "recovered")
	}
	// New writes continue above the recovered sequences.
	if err := db2.Put([]byte("post"), []byte("recovery")); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	db3, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	checkGet(t, db3, "post", modelVal{val: []byte("recovery")}, "post-recovery")
	for k, mv := range model {
		checkGet(t, db3, k, mv, "post-recovery")
	}
}

func TestDBTornWAL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// Find the live WAL and garble its tail.
	logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(logs) != 1 {
		t.Fatalf("expected one WAL, found %v", logs)
	}
	f, err := os.OpenFile(logs[0], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x40, 0x00, 0x00, 0x00, 0xde, 0xad})
	f.Close()

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen after torn tail: %v", err)
	}
	defer db2.Close()
	for i := 0; i < 50; i++ {
		checkGet(t, db2, fmt.Sprintf("k%02d", i), modelVal{val: []byte("v")}, "torn")
	}
}

func TestDBSnapshots(t *testing.T) {
	db, _ := openTest(t, Options{WriteBuffer: 16 << 10, L0CompactionTrigger: 2, TargetFileSize: 8 << 10})
	defer db.Close()

	put := func(k, v string) {
		t.Helper()
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	put("a", "1")
	put("b", "1")
	s1 := db.seq.Load()
	put("a", "2")
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	s2 := db.seq.Load()
	put("c", "3")
	stepEngine(t, db)
	put("a", "3")

	// Old snapshot: sees the first generation.
	v, ok, err := db.GetAt([]byte("a"), s1)
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("a@s1: %q %v %v", v, ok, err)
	}
	if _, ok, _ := db.GetAt([]byte("c"), s1); ok {
		t.Fatal("c visible at s1")
	}
	// Middle snapshot: sees the tombstone on b.
	v, ok, _ = db.GetAt([]byte("a"), s2)
	if !ok || string(v) != "2" {
		t.Fatalf("a@s2: %q %v", v, ok)
	}
	if _, ok, _ := db.GetAt([]byte("b"), s2); ok {
		t.Fatal("b@s2 should be deleted")
	}
	// Head: b gone, a is 3.
	if _, ok, _ := db.Get([]byte("b")); ok {
		t.Fatal("b should be deleted at head")
	}
	v, ok, _ = db.Get([]byte("a"))
	if !ok || string(v) != "3" {
		t.Fatalf("a@head: %q %v", v, ok)
	}

	// Scans at snapshots.
	it, err := db.NewIterAt(s1)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var got []string
	for it.First(); it.Valid(); it.Next() {
		got = append(got, string(it.UserKey()))
	}
	if strings.Join(got, ",") != "a,b" {
		t.Fatalf("scan@s1: %v", got)
	}
	it2, err := db.NewIterAt(s2)
	if err != nil {
		t.Fatal(err)
	}
	defer it2.Close()
	got = nil
	for it2.First(); it2.Valid(); it2.Next() {
		got = append(got, string(it2.UserKey()))
	}
	if strings.Join(got, ",") != "a" {
		t.Fatalf("scan@s2: %v", got)
	}
	// Seek positioning under a snapshot: past every visible key it
	// misses and leaves the iterator invalid, but still usable.
	if it2.Seek([]byte("aa")) {
		t.Fatalf("seek aa found %q; want miss", it2.UserKey())
	}
	if it2.Valid() {
		t.Fatal("iterator valid after a missed seek")
	}
	if !it2.Seek([]byte("a")) || string(it2.UserKey()) != "a" {
		t.Fatalf("seek a after a miss landed on %q", it2.UserKey())
	}
}

func TestDBScanSeek(t *testing.T) {
	db, _ := openTest(t, smallOpts())
	defer db.Close()
	for i := 0; i < 200; i++ {
		if err := db.Put([]byte(fmt.Sprintf("s%04d", i)), []byte(fmt.Sprintf("v%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
	stepEngine(t, db)
	it, err := db.NewIter()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	for probe := 0; probe < 200; probe++ {
		target := fmt.Sprintf("s%04d", probe)
		if !it.Seek([]byte(target)) {
			t.Fatalf("seek %s missed", target)
		}
		if string(it.UserKey()) != target {
			t.Fatalf("seek %s landed on %s", target, it.UserKey())
		}
	}
	if it.Seek([]byte("s0199x")) {
		t.Fatalf("seek past all landed on %q; want miss", it.UserKey())
	}
	if it.Valid() {
		t.Fatal("iterator valid after seek past all")
	}
	// Prefix-family ordering: "s01" < "s0100"-style checks are the
	// model test's job; here verify a seek before everything.
	if !it.Seek([]byte("a")) || string(it.UserKey()) != "s0000" {
		t.Fatalf("seek a landed on %q", it.UserKey())
	}
}

func TestDBConcurrent(t *testing.T) {
	db, _ := openTest(t, smallOpts())
	defer db.Close()
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("g%d-%04d", g, rng.Intn(400))
				if rng.Intn(5) == 0 {
					db.Delete([]byte(k))
				} else {
					db.Put([]byte(k), []byte(strings.Repeat("x", rng.Intn(64))))
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			it, err := db.NewIter()
			if err != nil {
				continue
			}
			for it.First(); it.Valid(); it.Next() {
			}
			it.Close()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			db.Get([]byte(fmt.Sprintf("g0-%04d", i%400)))
		}
	}()
	wg.Wait()
	if err := db.bgFailure(); err != nil {
		t.Fatalf("background error: %v", err)
	}
}
