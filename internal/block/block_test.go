package block

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func testEntries(n int, seed int64) [][2][]byte {
	rng := rand.New(rand.NewSource(seed))
	out := make([][2][]byte, 0, n)
	var key []byte
	for i := 0; i < n; i++ {
		key = append(key[:0:0], []byte(fmt.Sprintf("user%06d", i))...)
		// internal trailer: descending seq so raw order ascends
		key = append(key, byte(n-i), 0, 0, 0, 0, 0, 0, 1)
		val := make([]byte, rng.Intn(64))
		rng.Read(val)
		out = append(out, [2][]byte{key, val})
	}
	return out
}

func buildRaw(ents [][2][]byte) []byte {
	b := NewBuilder()
	for _, e := range ents {
		b.Add(e[0], e[1])
	}
	return b.Finish()
}

func TestRoundTrip(t *testing.T) {
	ents := testEntries(1000, 1)
	raw := buildRaw(ents)
	it, err := NewIter(raw)
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for it.First(); it.Valid(); i++ {
		if i >= len(ents) {
			t.Fatalf("iterator returned more entries than added")
		}
		if !bytes.Equal(it.Current().Key, ents[i][0]) {
			t.Fatalf("entry %d key mismatch", i)
		}
		if !bytes.Equal(it.Current().Value, ents[i][1]) {
			t.Fatalf("entry %d value mismatch", i)
		}
		it.Next()
	}
	if i != len(ents) {
		t.Fatalf("iterated %d entries, want %d", i, len(ents))
	}
	if it.Error() != nil {
		t.Fatal(it.Error())
	}
}

func TestSeek(t *testing.T) {
	ents := testEntries(500, 2)
	raw := buildRaw(ents)
	it, err := NewIter(raw)
	if err != nil {
		t.Fatal(err)
	}
	// Valid internal-key targets around entry i: same user key, trailer
	// value nudged so the target sorts strictly between neighbours.
	mkTarget := func(k []byte, trailer []byte) []byte {
		t := append([]byte(nil), k[:len(k)-8]...)
		return append(t, trailer...)
	}
	for i := 0; i < len(ents); i++ {
		key := ents[i][0]
		tr := key[len(key)-8:]
		before := append([]byte(nil), tr...)
		before[7] = tr[7] - 1 // smaller trailer sorts before entry i
		after := append([]byte(nil), tr...)
		after[7] = tr[7] + 1 // sorts after entry i, before entry i+1
		cases := []struct {
			name   string
			target []byte
			want   []byte
			hit    bool
		}{
			{"before", mkTarget(key, before), key, true},
			{"exact", append([]byte(nil), key...), key, true},
		}
		if i+1 < len(ents) {
			cases = append(cases, struct {
				name   string
				target []byte
				want   []byte
				hit    bool
			}{"after", mkTarget(key, after), ents[i+1][0], true})
		} else {
			cases = append(cases, struct {
				name   string
				target []byte
				want   []byte
				hit    bool
			}{"after", mkTarget(key, after), nil, false})
		}
		for _, c := range cases {
			ok := it.Seek(c.target)
			if ok != c.hit {
				t.Fatalf("seek i=%d %s: got %v want %v", i, c.name, ok, c.hit)
			}
			if ok && !bytes.Equal(it.Current().Key, c.want) {
				t.Fatalf("seek i=%d %s: landed on wrong key", i, c.name)
			}
		}
	}
	// Seek far past the end must miss.
	if it.Seek(bytes.Repeat([]byte{0xff}, 40)) {
		t.Fatal("seek past end should miss")
	}
}

func TestSeekMiss(t *testing.T) {
	ents := testEntries(100, 3)
	raw := buildRaw(ents)
	it, _ := NewIter(raw)
	// A key larger than everything must miss.
	if it.Seek(bytes.Repeat([]byte{0xff}, 40)) {
		t.Fatal("seek past end should miss")
	}
	// A valid internal key whose user part sorts before every stored
	// user key must land on the first entry.
	small := append([]byte("aaaa"), 0, 0, 0, 0, 0, 0, 0, 1)
	if !it.Seek(small) {
		t.Fatal("seek before first should land on first entry")
	}
	if !bytes.Equal(it.Current().Key, ents[0][0]) {
		t.Fatal("seek before first landed on the wrong entry")
	}
}

func TestCompressionActuallyCompresses(t *testing.T) {
	ents := testEntries(200, 4)
	raw := buildRaw(ents)
	rawSize := 0
	for _, e := range ents {
		rawSize += len(e[0]) + len(e[1])
	}
	if len(raw) >= rawSize {
		t.Fatalf("prefix compression gained nothing: block %d raw %d", len(raw), rawSize)
	}
}

func TestCorruptionDetected(t *testing.T) {
	ents := testEntries(50, 5)
	raw := buildRaw(ents)
	for _, flip := range []int{0, len(raw) / 2, len(raw) - 1} {
		bad := append([]byte(nil), raw...)
		bad[flip] ^= 0x01
		if _, err := NewIter(bad); err == nil {
			t.Fatalf("flip at %d: checksum not detected", flip)
		}
	}
}

func TestTruncatedRejected(t *testing.T) {
	ents := testEntries(10, 6)
	raw := buildRaw(ents)
	for _, n := range []int{0, 3, 7, len(raw) - 1} {
		if _, err := NewIter(raw[:n]); err == nil {
			t.Fatalf("truncation to %d bytes accepted", n)
		}
	}
}

func TestOutOfOrderPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("out of order Add should panic")
		}
	}()
	b := NewBuilder()
	b.Add([]byte("bbb"), nil)
	b.Add([]byte("aaa"), nil)
}

func TestRestartSearchAcrossBoundaries(t *testing.T) {
	// Enough entries to span several restart points; seek must still be
	// exact at every restart boundary.
	ents := testEntries(RestartInterval*5+3, 7)
	raw := buildRaw(ents)
	it, _ := NewIter(raw)
	for _, e := range ents {
		if !it.Seek(e[0]) {
			t.Fatalf("seek miss for present key")
		}
	}
}
