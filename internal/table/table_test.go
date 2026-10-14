package table

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/talaria0101/moraine/internal/key"
)

type entry struct {
	k []byte // user key
	s uint64
	t key.Type
	v []byte
}

func (e entry) ikey() []byte { return key.Encode(nil, e.k, e.s, e.t) }

func randEntries(n int, seed int64, valSize int) []entry {
	rng := rand.New(rand.NewSource(seed))
	out := make([]entry, 0, n)
	for i := 0; i < n; i++ {
		v := make([]byte, rng.Intn(valSize+1))
		rng.Read(v)
		out = append(out, entry{
			k: []byte(fmt.Sprintf("user%06d", rng.Intn(n))),
			s: uint64(rng.Intn(1 << 30)),
			t: key.TypeSet,
			v: v,
		})
	}
	// internal order: user asc, seq desc; sort stable by encoding then
	// dedupe exact duplicates (same key+seq must not repeat)
	enc := make(map[string]int)
	var out2 []entry
	for _, e := range out {
		out2 = append(out2, e)
	}
	// build sorted view
	sortEntries(out2)
	// dedupe identical (user,seq)
	out3 := out2[:0]
	for i, e := range out2 {
		id := fmt.Sprintf("%s/%d", e.k, e.s)
		if _, dup := enc[id]; dup {
			continue
		}
		enc[id] = i
		out3 = append(out3, e)
	}
	return out3
}

func sortEntries(es []entry) {
	// insertion of a comparator via sort.Slice
	type pair struct {
		e entry
		i []byte
	}
	ps := make([]pair, len(es))
	for i, e := range es {
		ps[i] = pair{e, e.ikey()}
	}
	for i := 1; i < len(ps); i++ {
		for j := i; j > 0 && bytes.Compare(ps[j].i, ps[j-1].i) < 0; j-- {
			ps[j], ps[j-1] = ps[j-1], ps[j]
		}
	}
	for i := range ps {
		es[i] = ps[i].e
	}
}

func buildTable(t *testing.T, es []entry, opts Options) (string, Meta) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.mrn")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := NewWriter(f, opts)
	for _, e := range es {
		w.Add(e.ikey(), e.v)
	}
	meta, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return path, meta
}

func openTable(t *testing.T, path string) *Reader {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(f, uint64(st.Size()))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestTableRoundTripAndScan(t *testing.T) {
	es := randEntries(2000, 42, 100)
	path, meta := buildTable(t, es, Options{})
	if meta.Count != uint64(len(es)) {
		t.Fatalf("count %d want %d", meta.Count, len(es))
	}
	r := openTable(t, path)
	defer func() { t.Cleanup(func() {}) }()
	it := r.NewIter()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		ik, val := it.Current()
		want := es[n]
		if !bytes.Equal(ik.UserKey, want.k) || ik.Seq != want.s || ik.Type != want.t {
			t.Fatalf("entry %d key mismatch", n)
		}
		if !bytes.Equal(val, want.v) {
			t.Fatalf("entry %d value mismatch", n)
		}
		n++
	}
	if n != len(es) {
		t.Fatalf("scanned %d, want %d", n, len(es))
	}
	if it.Error() != nil {
		t.Fatal(it.Error())
	}
}

func TestTableGetWithSnapshots(t *testing.T) {
	// Several versions of a handful of keys, newest first as the format
	// requires.
	var es []entry
	for s := uint64(10); s >= 1; s-- {
		es = append(es, entry{k: []byte("alpha"), s: s, t: key.TypeSet, v: []byte(fmt.Sprintf("v%d", s))})
	}
	es = append(es, entry{k: []byte("beta"), s: 9, t: key.TypeSet, v: []byte("nine")})
	es = append(es, entry{k: []byte("beta"), s: 5, t: key.TypeDelete})
	es = append(es, entry{k: []byte("gamma"), s: 3, t: key.TypeSet, v: []byte("g")})
	path, _ := buildTable(t, es, Options{BlockSize: 128})
	r := openTable(t, path)

	cases := []struct {
		key  string
		snap uint64
		want string
		have bool
		typ  key.Type
	}{
		{"alpha", 1, "v1", true, key.TypeSet},
		{"alpha", 5, "v5", true, key.TypeSet},
		{"alpha", 10, "v10", true, key.TypeSet},
		{"alpha", 100, "v10", true, key.TypeSet},
		{"beta", 4, "", false, 0},
		{"beta", 5, "", true, key.TypeDelete},
		{"beta", 8, "", true, key.TypeDelete},
		{"beta", 9, "nine", true, key.TypeSet},
		{"gamma", 3, "g", true, key.TypeSet},
		{"gamma", 2, "", false, 0},
		{"delta", 99, "", false, 0},
	}
	for _, c := range cases {
		ik, val, ok, err := r.Get([]byte(c.key), c.snap)
		if err != nil {
			t.Fatal(err)
		}
		if ok != c.have {
			t.Fatalf("get %q snap %d: found=%v want %v", c.key, c.snap, ok, c.have)
		}
		if !ok {
			continue
		}
		if ik.Type != c.typ || string(val) != c.want {
			t.Fatalf("get %q snap %d: got type %d val %q want %q", c.key, c.snap, ik.Type, val, c.want)
		}
	}
}

func TestTableVersionsStraddleBlocks(t *testing.T) {
	// Tiny blocks force many blocks; write many versions of one key
	// (newest first) and check Get still finds each snapshot, including
	// when versions are split across the boundary.
	var es []entry
	for s := uint64(100); s >= 1; s-- {
		es = append(es, entry{k: []byte("solo"), s: s, t: key.TypeSet, v: []byte(fmt.Sprintf("v%03d", s))})
	}
	path, _ := buildTable(t, es, Options{BlockSize: 256})
	r := openTable(t, path)
	for _, snap := range []uint64{1, 50, 51, 99, 100, 1000} {
		want := uint64(100)
		if snap < 100 {
			want = snap
		}
		ik, val, ok, err := r.Get([]byte("solo"), snap)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || ik.Seq != want || string(val) != fmt.Sprintf("v%03d", want) {
			t.Fatalf("snap %d: got (%d,%q,%v)", snap, ik.Seq, val, ok)
		}
	}
}

func TestTableSeek(t *testing.T) {
	es := randEntries(500, 7, 30)
	path, _ := buildTable(t, es, Options{BlockSize: 512})
	r := openTable(t, path)
	it := r.NewIter()
	rng := rand.New(rand.NewSource(99))
	cmpIkeys := func(i int, target []byte) bool {
		return key.Compare(es[i].ikey(), target) >= 0
	}
	for probe := 0; probe < 2000; probe++ {
		var target []byte
		switch rng.Intn(3) {
		case 0: // exact entry
			target = es[rng.Intn(len(es))].ikey()
		case 1: // same user, phantom sequence
			e := es[rng.Intn(len(es))]
			target = key.Encode(nil, e.k, e.s+rng.Uint64()%(1<<20), key.TypeSet)
		case 2: // phantom user
			target = key.Encode(nil, []byte(fmt.Sprintf("ghost%06d", rng.Intn(len(es)))), rng.Uint64()%(1<<30), key.TypeSet)
		}
		want := -1
		lo, hi := 0, len(es)
		for lo < hi {
			mid := int(uint(lo+hi) / 2)
			if cmpIkeys(mid, target) {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		if lo < len(es) {
			want = lo
		}
		ok := it.Seek(target)
		if (want >= 0) != ok {
			t.Fatalf("probe %d: seek=%v want idx %d", probe, ok, want)
		}
		if ok {
			ik, _ := it.Current()
			got := key.Encode(nil, ik.UserKey, ik.Seq, ik.Type)
			if key.Compare(es[want].ikey(), got) != 0 {
				t.Fatalf("probe %d: landed wrong", probe)
			}
		}
	}
}

func TestTableCorruption(t *testing.T) {
	es := randEntries(200, 9, 40)
	path, _ := buildTable(t, es, Options{})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// flip a byte in the middle of a data block
	bad := append([]byte(nil), raw...)
	bad[len(bad)/2] ^= 0x40
	badPath := filepath.Join(t.TempDir(), "bad.mrn")
	if err := os.WriteFile(badPath, bad, 0o644); err != nil {
		t.Fatal(err)
	}
	r := openTable(t, badPath)
	it := r.NewIter()
	sawErr := false
	for it.First(); it.Valid(); it.Next() {
	}
	if it.Error() != nil {
		sawErr = true
	}
	if !sawErr {
		// The flip may have hit the filter or index; either way Open or
		// iteration must have failed. If neither did, the byte was in a
		// region with redundancy, which this layout does not have.
		t.Fatal("corruption not detected anywhere")
	}
}

func TestTableTruncatedFooter(t *testing.T) {
	es := randEntries(10, 3, 8)
	path, _ := buildTable(t, es, Options{})
	raw, _ := os.ReadFile(path)
	if _, err := Open(bytes.NewReader(raw[:len(raw)-10]), uint64(len(raw)-10)); err == nil {
		t.Fatal("truncated file accepted")
	}
}

func TestTableMetaBounds(t *testing.T) {
	es := randEntries(300, 11, 50)
	path, meta := buildTable(t, es, Options{})
	r := openTable(t, path)
	ikS, _ := key.Decode(meta.Smallest)
	ikL, _ := key.Decode(meta.Largest)
	if string(ikS.UserKey) != string(es[0].k) || string(ikL.UserKey) != string(es[len(es)-1].k) {
		t.Fatal("meta bounds wrong")
	}
	// point lookup honors bounds: nothing outside exists
	if _, _, ok, _ := r.Get([]byte("zzzzzz"), 1<<40); ok {
		t.Fatal("found key outside bounds")
	}
}
