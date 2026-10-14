package wal

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/talaria0101/moraine/internal/key"
)

func mkBatch(seq uint64, n int) *Batch {
	b := &Batch{Seq: seq}
	for i := 0; i < n; i++ {
		b.Entries = append(b.Entries, Entry{
			Key:   []byte{byte('a' + byte(i%26)), byte(i)},
			Value: []byte{byte(i), byte(i >> 8), byte(seq)},
			Type:  key.TypeSet,
		})
	}
	return b
}

func writeAndRead(t *testing.T, batches []*Batch) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range batches {
		if err := w.Append(b, i%2 == 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, want := range batches {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
		if got.Seq != want.Seq || len(got.Entries) != len(want.Entries) {
			t.Fatalf("batch %d mismatch: seq %d/%d entries %d/%d",
				i, got.Seq, want.Seq, len(got.Entries), len(want.Entries))
		}
		for j := range want.Entries {
			if !bytes.Equal(got.Entries[j].Key, want.Entries[j].Key) ||
				!bytes.Equal(got.Entries[j].Value, want.Entries[j].Value) ||
				got.Entries[j].Type != want.Entries[j].Type {
				t.Fatalf("batch %d entry %d mismatch", i, j)
			}
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	return path
}

func TestWALRoundTrip(t *testing.T) {
	var batches []*Batch
	for i := 0; i < 50; i++ {
		batches = append(batches, mkBatch(uint64(i*10), i%7+1))
	}
	writeAndRead(t, batches)
}

func TestWALRoundTripEmptyBatch(t *testing.T) {
	writeAndRead(t, []*Batch{{Seq: 1}, mkBatch(2, 3), {Seq: 3}})
}

func TestWALTornTail(t *testing.T) {
	path := writeAndRead(t, []*Batch{mkBatch(1, 5), mkBatch(2, 5), mkBatch(3, 5)})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Each record is exactly 50 bytes here (5 entries x 8 bytes + 10
	// bytes of batch header and record header). Truncating at cut must
	// replay exactly the records fully contained: cut/50.
	for cut := len(raw) - 12; cut < len(raw); cut++ {
		trunc := append([]byte(nil), raw[:cut]...)
		tp := filepath.Join(t.TempDir(), "torn.wal")
		if err := os.WriteFile(tp, trunc, 0o644); err != nil {
			t.Fatal(err)
		}
		r, err := OpenReader(tp)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for {
			b, err := r.Next()
			if err != nil {
				break
			}
			if b.Seq != uint64(n+1) {
				t.Fatalf("cut %d: batch %d has seq %d", cut, n, b.Seq)
			}
			n++
		}
		r.Close()
		if n != cut/50 {
			t.Fatalf("cut %d: replayed %d batches, want %d", cut, n, cut/50)
		}
	}
}

func TestWALTruncateAfterTorn(t *testing.T) {
	path := writeAndRead(t, []*Batch{mkBatch(1, 5), mkBatch(2, 5)})
	raw, _ := os.ReadFile(path)
	// Corrupt a byte inside the second record's payload.
	second := int64(0)
	{
		r, _ := OpenReader(path)
		r.Next()
		second = r.GoodEnd()
		r.Close()
	}
	bad := append([]byte(nil), raw...)
	bad[second+9] ^= 0xff // inside second record payload
	bp := filepath.Join(t.TempDir(), "corrupt.wal")
	os.WriteFile(bp, bad, 0o644)
	r, err := OpenReader(bp)
	if err != nil {
		t.Fatal(err)
	}
	b1, err := r.Next()
	if err != nil || b1.Seq != 1 {
		t.Fatalf("first batch: %v %v", b1, err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("checksummed corruption must read as torn tail, got %v", err)
	}
	if r.GoodEnd() != second {
		t.Fatalf("goodEnd %d want %d", r.GoodEnd(), second)
	}
	r.Close()
	// Simulate recovery truncation, then replay is a clean prefix.
	if err := os.Truncate(bp, second); err != nil {
		t.Fatal(err)
	}
	r2, _ := OpenReader(bp)
	b, err := r2.Next()
	if err != nil || b.Seq != 1 {
		t.Fatalf("post-truncate replay: %v %v", b, err)
	}
	if _, err := r2.Next(); err != io.EOF {
		t.Fatal("post-truncate replay must end cleanly")
	}
	r2.Close()
}

func TestWALRejectsOversizedBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	big := &Batch{Seq: 1, Entries: []Entry{{
		Key:   []byte("k"),
		Value: make([]byte, MaxBatchBytes),
		Type:  key.TypeSet,
	}}}
	if err := w.Append(big, false); err == nil {
		t.Fatal("oversized batch accepted")
	}
}
