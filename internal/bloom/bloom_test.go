package bloom

import (
	"bytes"
	"fmt"
	"testing"
)

func TestNoFalseNegatives(t *testing.T) {
	for _, bpk := range []int{1, 4, 10, 16} {
		var keys [][]byte
		for i := 0; i < 10000; i++ {
			keys = append(keys, []byte(fmt.Sprintf("key-%d-%s", i, bytes.Repeat([]byte("x"), i%17))))
		}
		f := New(keys, bpk)
		for _, k := range keys {
			if !f.MayContain(k) {
				t.Fatalf("bitsPerKey %d: false negative", bpk)
			}
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	// With 10 bits per key the optimum-k FPR is about 0.0082. The
	// measured rate on 10000 absent keys must stay under 0.02, a loose
	// bound that catches both broken hashing and an off-by-one in k.
	var keys [][]byte
	for i := 0; i < 10000; i++ {
		keys = append(keys, []byte(fmt.Sprintf("present-%d", i)))
	}
	f := New(keys, 10)
	hits := 0
	trials := 20000
	for i := 0; i < trials; i++ {
		if f.MayContain([]byte(fmt.Sprintf("absent-%d", i))) {
			hits++
		}
	}
	rate := float64(hits) / float64(trials)
	if rate > 0.02 {
		t.Fatalf("FPR %.4f exceeds 0.02 (hits %d/%d)", rate, hits, trials)
	}
	t.Logf("FPR at 10 bits/key: %.4f", rate)
}

func TestNilFilterAlwaysMaybe(t *testing.T) {
	var f *Filter
	if !f.MayContain([]byte("anything")) {
		t.Fatal("nil filter must answer true")
	}
	if f.Encode(nil) != nil {
		t.Fatal("nil filter encodes to nothing")
	}
}

func TestRoundTrip(t *testing.T) {
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	f := New(keys, 10)
	enc := f.Encode(nil)
	got, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !got.MayContain(k) {
			t.Fatalf("decoded filter lost key %q", k)
		}
	}
	if got.MayContain([]byte("zzzzzzzz")) {
		t.Log("note: false positive on zzzzzzzz (possible)")
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte{1, 2, 3}); err == nil {
		t.Fatal("short payload accepted")
	}
	bad := New([][]byte{[]byte("k")}, 10).Encode(nil)
	bad[0] = 0xff // numProbes out of range
	if _, err := Decode(bad); err == nil {
		t.Fatal("bad numProbes accepted")
	}
}
