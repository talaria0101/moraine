package mem

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/talaria0101/moraine/internal/key"
)

func put(t *Table, user string, seq uint64, val string) {
	t.Put(key.Encode(nil, []byte(user), seq, key.TypeSet), []byte(val))
}

func get(t *Table, user string, snap uint64) (string, bool) {
	ik, v, ok := t.Get([]byte(user), snap)
	return string(v) + "/" + fmt.Sprint(ik.Seq), ok
}

func TestSkiplistOrderAndSnapshots(t *testing.T) {
	tb := New()
	// insert newest-first per key is NOT required for a memtable (it
	// orders internally), so insert in arbitrary order.
	put(tb, "b", 5, "b5")
	put(tb, "a", 10, "a10")
	put(tb, "a", 3, "a3")
	put(tb, "a", 7, "a7")
	put(tb, "c", 1, "c1")

	if v, ok := get(tb, "a", 100); !ok || v != "a10/10" {
		t.Fatalf("latest a: %q %v", v, ok)
	}
	if v, ok := get(tb, "a", 7); !ok || v != "a7/7" {
		t.Fatalf("a at 7: %q %v", v, ok)
	}
	if v, ok := get(tb, "a", 6); !ok || v != "a3/3" {
		t.Fatalf("a at 6: %q %v", v, ok)
	}
	if _, ok := get(tb, "a", 2); ok {
		t.Fatal("a at 2 should miss")
	}
	if _, ok := get(tb, "d", 100); ok {
		t.Fatal("d should miss")
	}
	if tb.Len() != 5 {
		t.Fatalf("len %d", tb.Len())
	}
}

func TestSkiplistIterSorted(t *testing.T) {
	tb := New()
	rng := rand.New(rand.NewSource(3))
	want := map[string]string{}
	var users []string
	for i := 0; i < 5000; i++ {
		u := fmt.Sprintf("k%05d", rng.Intn(800))
		v := fmt.Sprintf("v%d", i)
		if _, seen := want[u]; !seen {
			users = append(users, u)
		}
		tb.Put(key.Encode(nil, []byte(u), uint64(i), key.TypeSet), []byte(v))
		want[u] = v // i increases so the last write is the newest
	}
	it := tb.NewIter()
	n := 0
	var prev []byte
	for it.First(); it.Valid(); it.Next() {
		ik, _ := it.Current()
		if n > 0 && key.CompareUser(prev, ik.UserKey) > 0 {
			t.Fatalf("user keys not ascending at %d", n)
		}
		prev = append(prev[:0], ik.UserKey...)
		n++
	}
	if n != tb.Len() {
		t.Fatalf("iterated %d, table holds %d", n, tb.Len())
	}
	// every user key must be present with its latest value
	for _, u := range users {
		ik, v, ok := tb.Get([]byte(u), 1<<40)
		if !ok || string(v) != want[u] || string(ik.UserKey) != u {
			t.Fatalf("get %q: %q %q %v", u, v, ik.UserKey, ok)
		}
	}
}

// userOnly copies just the user key out of an internal key.
func userOnly(b []byte) []byte { return append([]byte(nil), b...) }

func TestSkiplistSeek(t *testing.T) {
	tb := New()
	for i := 0; i < 100; i++ {
		put(tb, fmt.Sprintf("k%04d", i), uint64(1000-i), "v")
	}
	it := tb.NewIter()
	if !it.Seek(targetFor([]byte("k0050"))) {
		t.Fatal("seek miss")
	}
	ik, _ := it.Current()
	if string(ik.UserKey) != "k0050" {
		t.Fatalf("seek landed on %q", ik.UserKey)
	}
	// between keys
	if !it.Seek(key.Encode(nil, []byte("k0050x"), 0, key.TypeDelete)) {
		t.Fatal("seek miss between")
	}
	ik, _ = it.Current()
	if string(ik.UserKey) != "k0051" {
		t.Fatalf("between seek landed on %q", ik.UserKey)
	}
	// past end
	if it.Seek(key.Encode(nil, []byte("zzz"), 0, key.TypeDelete)) {
		t.Fatal("seek past end should miss")
	}
}

func TestSkiplistConcurrent(t *testing.T) {
	tb := New()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				tb.Put(key.Encode(nil, []byte(fmt.Sprintf("k%d", i%501)), uint64(i), key.TypeSet), []byte("v"))
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				tb.Get([]byte(fmt.Sprintf("k%d", i%501)), 1<<40)
				tb.Size()
				tb.Len()
			}
		}()
	}
	wg.Wait()
}

func TestSkiplistHeightDistribution(t *testing.T) {
	// A crude sanity check on the RNG: with p=1/4 the expected average
	// height is 1/(1-1/4) = 1.333. Sample heights via insertion of many
	// nodes and checking size accounting remains near linear.
	tb := New()
	n := 20000
	for i := 0; i < n; i++ {
		tb.Put(key.Encode(nil, []byte(fmt.Sprintf("k%06d", i)), uint64(i), key.TypeSet), nil)
	}
	if tb.Len() != n {
		t.Fatalf("len %d want %d", tb.Len(), n)
	}
	if s := tb.Size(); s < int64(n)*64 || s > int64(n)*200 {
		t.Fatalf("size %d for %d nodes is out of the sane band", s, n)
	}
}
