package manifest

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/talaria0101/moraine/internal/key"
)

func ik(user string, seq uint64) []byte {
	return key.Encode(nil, []byte(user), seq, key.TypeSet)
}

func meta(id uint64, level int, lo, hi string) TableMeta {
	return TableMeta{
		ID:       id,
		Level:    level,
		Size:     1000 + id,
		Count:    10 + id,
		Smallest: ik(lo, 5),
		Largest:  ik(hi, 7),
	}
}

func mustOpen(t *testing.T, path string) *Manifest {
	t.Helper()
	m, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return m
}

func mustApply(t *testing.T, m *Manifest, e *Edit) {
	t.Helper()
	if err := m.LogAndApply(e); err != nil {
		t.Fatalf("logAndApply: %v", err)
	}
}

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")
	m := mustOpen(t, path)
	want := []TableMeta{meta(1, 0, "a", "b"), meta(2, 1, "c", "d"), meta(3, 1, "e", "f")}
	mustApply(t, m, &Edit{Added: want, NextID: 4})
	mustApply(t, m, &Edit{Deleted: []uint64{2}})
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2 := mustOpen(t, path)
	defer m2.Close()
	v := m2.Current()
	var got []TableMeta
	for _, lv := range v.Levels {
		got = append(got, lv...)
	}
	if len(got) != 2 {
		t.Fatalf("replayed %d tables, want 2", len(got))
	}
	if got[0].ID != 1 || got[1].ID != 3 {
		t.Fatalf("replayed wrong ids: %v %v", got[0].ID, got[1].ID)
	}
	if m2.NextFileID() != 4 {
		t.Fatalf("nextID %d, want 4", m2.NextFileID())
	}
}

func TestManifestTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")
	m := mustOpen(t, path)
	mustApply(t, m, &Edit{Added: []TableMeta{meta(1, 0, "a", "b")}, NextID: 2})
	mustApply(t, m, &Edit{Added: []TableMeta{meta(2, 0, "c", "d")}, NextID: 3})
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: half a record at the tail.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x09, 0x00, 0x00}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	m2 := mustOpen(t, path)
	v := m2.Current()
	if n := len(v.Levels[0]); n != 2 {
		t.Fatalf("replayed %d tables, want the 2 complete ones", n)
	}
	// The manifest must be appendable after the truncation.
	mustApply(t, m2, &Edit{Added: []TableMeta{meta(5, 0, "e", "f")}})
	if m2.NextFileID() != 6 {
		t.Fatalf("nextID %d, want 6", m2.NextFileID())
	}
	if err := m2.Close(); err != nil {
		t.Fatal(err)
	}

	m3 := mustOpen(t, path)
	defer m3.Close()
	if n := len(m3.Current().Levels[0]); n != 3 {
		t.Fatalf("after recovery append: %d tables, want 3", n)
	}
}

func TestManifestCorruptMidLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")
	m := mustOpen(t, path)
	mustApply(t, m, &Edit{Added: []TableMeta{meta(1, 0, "a", "b")}, NextID: 2})
	sizeAfterFirst, _ := os.Stat(path)
	_ = sizeAfterFirst
	mustApply(t, m, &Edit{Added: []TableMeta{meta(2, 0, "c", "d")}, NextID: 3})
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Flip the last byte of the second record (the add for meta 2),
	// not the last byte of the file (the next-id record).
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rec2 := 8 + int(binary.LittleEndian.Uint32(raw[0:4]))
	raw[rec2+8-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	m2 := mustOpen(t, path)
	defer m2.Close()
	if n := len(m2.Current().Levels[0]); n != 1 {
		t.Fatalf("replayed %d tables, want 1 (records after corruption dropped)", n)
	}
}

func TestManifestSnapshotKeepsNextID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")
	m := mustOpen(t, path)
	// High IDs, all deleted before the snapshot: a snapshot that only
	// records live tables would let nextID regress on recovery.
	mustApply(t, m, &Edit{Added: []TableMeta{meta(1, 0, "a", "b"), meta(99, 0, "c", "d")}, NextID: 100})
	mustApply(t, m, &Edit{Deleted: []uint64{1, 99}})
	if err := m.RewriteSnapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() > 512 {
		t.Fatalf("snapshot left %d bytes, rewrite did not compact the log", fi.Size())
	}

	m2 := mustOpen(t, path)
	defer m2.Close()
	if n := len(m2.Current().FileIDs()); n != 0 {
		t.Fatalf("snapshot replayed %d tables, want 0", n)
	}
	if m2.NextFileID() != 100 {
		t.Fatalf("nextID %d after snapshot, want 100", m2.NextFileID())
	}
}

func TestVersionOverlaps(t *testing.T) {
	v := NewVersion()
	m1 := meta(1, 0, "b", "d")
	m2 := meta(2, 1, "a", "c")
	m3 := meta(3, 1, "f", "h")
	m4 := meta(4, 1, "j", "l")
	v = apply(v, &Edit{Added: []TableMeta{m1, m2, m3, m4}})

	got := v.Overlaps(0, []byte("a"), []byte("z"))
	if len(got) != 1 || got[0].ID != 1 {
		t.Fatalf("L0 overlaps: got %v", got)
	}
	got = v.Overlaps(1, []byte("b"), []byte("g"))
	if len(got) != 2 || got[0].ID != 2 || got[1].ID != 3 {
		t.Fatalf("L1 overlaps: got %v", got)
	}
	got = v.Overlaps(1, []byte("i"), []byte("k"))
	if len(got) != 1 || got[0].ID != 4 {
		t.Fatalf("L1 overlaps 2: got %v", got)
	}
	if got := v.Overlaps(1, []byte("m"), []byte("z")); len(got) != 0 {
		t.Fatalf("empty range: got %v", got)
	}
	// Deeper levels must be binary-searchable by point.
	if m, ok := v.FindFile(1, []byte("g")); !ok || m.ID != 3 {
		t.Fatalf("FindFile(g): %v %v", m, ok)
	}
	if _, ok := v.FindFile(1, []byte("e")); ok {
		t.Fatal("FindFile(e) should miss the gap between c and f")
	}
}

func TestManifestRandomizedReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")
	rng := rand.New(rand.NewSource(4242))

	live := map[uint64]TableMeta{}
	nextID := uint64(1)
	m := mustOpen(t, path)
	for round := 0; round < 200; round++ {
		e := &Edit{}
		for add := 0; add < 1+rng.Intn(4); add++ {
			mt := meta(nextID, rng.Intn(4),
				fmt.Sprintf("k%04d", rng.Intn(100)),
				fmt.Sprintf("k%04d", 100+rng.Intn(100)))
			e.Added = append(e.Added, mt)
			live[nextID] = mt
			nextID++
		}
		for del := 0; del < rng.Intn(3) && len(live) > 0; del++ {
			for id := range live {
				e.Deleted = append(e.Deleted, id)
				delete(live, id)
				break
			}
		}
		e.NextID = nextID
		mustApply(t, m, e)
		if rng.Intn(10) == 0 {
			if err := m.RewriteSnapshot(); err != nil {
				t.Fatalf("snapshot: %v", err)
			}
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2 := mustOpen(t, path)
	defer m2.Close()
	if m2.NextFileID() != nextID {
		t.Fatalf("nextID %d, want %d", m2.NextFileID(), nextID)
	}
	got := map[uint64]bool{}
	for _, id := range m2.Current().FileIDs() {
		got[id] = true
	}
	if len(got) != len(live) {
		t.Fatalf("replayed %d live tables, want %d", len(got), len(live))
	}
	for id := range live {
		if !got[id] {
			t.Fatalf("table %d missing after replay", id)
		}
	}
}

// TestManifestWatermarksThroughClone proves the cloneEdit path carries
// both watermarks into LogAndApply. Dropping NextSeq there used to
// reset the recovery sequence on every reopen, making every flushed
// entry invisible to new readers.
func TestManifestWatermarksThroughClone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	m := mustOpen(t, path)
	mustApply(t, m, &Edit{
		Added:   []TableMeta{meta(1, 0, "a", "m")},
		NextID:  50,
		NextSeq: 1000,
	})
	if got := m.NextFileID(); got != 50 {
		t.Fatalf("NextFileID in-process: got %d, want 50", got)
	}
	if got := m.RecoveredNextSeq(); got != 1000 {
		t.Fatalf("RecoveredNextSeq in-process: got %d, want 1000", got)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	m2 := mustOpen(t, path)
	defer m2.Close()
	if got := m2.NextFileID(); got != 50 {
		t.Fatalf("NextFileID after reopen: got %d, want 50", got)
	}
	if got := m2.RecoveredNextSeq(); got != 1000 {
		t.Fatalf("RecoveredNextSeq after reopen: got %d, want 1000", got)
	}
}
