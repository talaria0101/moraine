// Package manifest tracks the live set of tables across levels. The
// manifest is an append-only log of edits (tables added, tables
// dropped); replaying it rebuilds the current Version, which is an
// immutable snapshot handed to readers. When the log grows past a
// threshold it is rewritten as a single snapshot record set, so
// recovery cost stays bounded by live state rather than total history.
//
// Durability: edits are appended and fsynced before the in-memory
// version is published, and the snapshot rewrite is a temp-file
// fsync-rename-fsync-dir sequence. A crash mid-append leaves a torn
// final record, which replay discards; a crash mid-rewrite leaves the
// old manifest intact under rename atomicity.
package manifest

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/talaria0101/moraine/internal/key"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record tags.
const (
	recAdd     = 1
	recDelete  = 2
	recNextID  = 3
	recNextSeq = 4
)

// DefaultSnapshotBytes is the manifest log size at which LogAndApply
// rewrites the log as a snapshot.
const DefaultSnapshotBytes = 4 << 20

// TableMeta describes one live table file.
type TableMeta struct {
	ID       uint64 // file number, also the name on disk (NNNNNN.sst)
	Level    int
	Size     uint64
	Count    uint64
	Smallest []byte // internal key
	Largest  []byte // internal key
}

// Edit is a set of changes to apply atomically: the new version either
// contains every Added table and none of the Deleted ones, or none of
// the edit happened.
type Edit struct {
	Added   []TableMeta
	Deleted []uint64 // table IDs
	NextID  uint64   // persisted so file numbers never regress after a snapshot; 0 leaves it alone
	NextSeq uint64   // persisted sequence watermark so recovered WAL entries never reuse a sequence already in a table; 0 leaves it alone
}

// Version is an immutable set of live tables, one slice per level.
// Level 0 holds tables sorted newest first (highest ID first); every
// other level is sorted by Smallest internal key. Readers grab the
// pointer once and can rely on the contents never changing.
type Version struct {
	Levels [][]TableMeta
}

// NewVersion returns the empty version.
func NewVersion() *Version {
	return &Version{}
}

// ensureLevels grows the level slices so index l exists.
func (v *Version) ensureLevels(l int) {
	for len(v.Levels) <= l {
		v.Levels = append(v.Levels, nil)
	}
}

// apply returns a new version with the edit applied. Table metadata is
// shared between versions; the per-level slices are fresh. Within one
// edit, Deleted wins over Added, so a table that is both added and
// deleted ends up absent.
func apply(cur *Version, e *Edit) *Version {
	deleted := make(map[uint64]bool, len(e.Deleted))
	for _, id := range e.Deleted {
		deleted[id] = true
	}
	n := len(cur.Levels)
	for _, m := range e.Added {
		if m.Level+1 > n {
			n = m.Level + 1
		}
	}
	next := &Version{Levels: make([][]TableMeta, n)}
	for i := 0; i < n; i++ {
		var lv []TableMeta
		if i < len(cur.Levels) {
			lv = cur.Levels[i]
		}
		keep := lv[:0:0]
		for _, m := range lv {
			if !deleted[m.ID] {
				keep = append(keep, m)
			}
		}
		for _, m := range e.Added {
			if m.Level == i && !deleted[m.ID] {
				keep = append(keep, m)
			}
		}
		next.Levels[i] = keep
	}
	// L0: newest first. Deeper levels: key order.
	if len(next.Levels) > 0 {
		l0 := next.Levels[0]
		sort.SliceStable(l0, func(a, b int) bool { return l0[a].ID > l0[b].ID })
	}
	for l := 1; l < len(next.Levels); l++ {
		lv := next.Levels[l]
		sort.Slice(lv, func(a, b int) bool {
			return key.Compare(lv[a].Smallest, lv[b].Smallest) < 0
		})
	}
	return next
}

// FindFile binary searches a level >= 1 for the file whose range
// covers userKey.
func (v *Version) FindFile(level int, userKey []byte) (TableMeta, bool) {
	if level >= len(v.Levels) {
		return TableMeta{}, false
	}
	lv := v.Levels[level]
	i := sort.Search(len(lv), func(i int) bool {
		lu, _ := splitUser(lv[i].Largest)
		return key.CompareUser(lu, userKey) >= 0
	})
	if i == len(lv) {
		return TableMeta{}, false
	}
	m := lv[i]
	su, _ := splitUser(m.Smallest)
	lu, _ := splitUser(m.Largest)
	if key.CompareUser(su, userKey) <= 0 && key.CompareUser(userKey, lu) <= 0 {
		return m, true
	}
	return TableMeta{}, false
}

// Overlaps returns the files of level whose user key range intersects
// [minUser, maxUser] (inclusive). L0 is scanned linearly; deeper
// levels are contiguous and found by binary search.
func (v *Version) Overlaps(level int, minUser, maxUser []byte) []TableMeta {
	if level >= len(v.Levels) {
		return nil
	}
	lv := v.Levels[level]
	if level == 0 {
		var out []TableMeta
		for _, m := range lv {
			if overlapsRange(m, minUser, maxUser) {
				out = append(out, m)
			}
		}
		return out
	}
	i := sort.Search(len(lv), func(i int) bool {
		lu, _ := splitUser(lv[i].Largest)
		return key.CompareUser(lu, minUser) >= 0
	})
	var out []TableMeta
	for ; i < len(lv); i++ {
		m := lv[i]
		su, _ := splitUser(m.Smallest)
		if key.CompareUser(su, maxUser) > 0 {
			break
		}
		out = append(out, m)
	}
	return out
}

func overlapsRange(m TableMeta, minUser, maxUser []byte) bool {
	su, _ := splitUser(m.Smallest)
	lu, _ := splitUser(m.Largest)
	return key.CompareUser(su, maxUser) <= 0 && key.CompareUser(minUser, lu) <= 0
}

func splitUser(ik []byte) ([]byte, bool) {
	if len(ik) < key.TrailerLen {
		return nil, false
	}
	return ik[:len(ik)-key.TrailerLen], true
}

// Manifest is the append-only log plus the current in-memory version.
type Manifest struct {
	path      string
	f         *os.File
	w         *bufio.Writer
	mu        sync.Mutex
	cur       atomic.Pointer[Version]
	nextID    uint64
	nextSeq   uint64
	logBytes  int64
	snapBytes int64
}

// Open replays the manifest at path, truncating any torn tail, and
// returns a handle positioned for appends. A missing file starts an
// empty manifest.
func Open(path string) (*Manifest, error) {
	m := &Manifest{
		path:      path,
		snapBytes: DefaultSnapshotBytes,
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	m.f = f
	cur := NewVersion()
	good, nextID, nextSeq, err := replay(f, cur)
	if err != nil {
		f.Close()
		return nil, err
	}
	m.cur.Store(cur)
	m.nextID = nextID
	m.nextSeq = nextSeq
	if fi, err := f.Stat(); err == nil {
		m.logBytes = fi.Size()
	} else {
		f.Close()
		return nil, err
	}
	if good < m.logBytes {
		// Torn or corrupt tail: drop it so the log is appendable.
		if err := f.Truncate(good); err != nil {
			f.Close()
			return nil, err
		}
		m.logBytes = good
	}
	if _, err := f.Seek(good, 0); err != nil {
		f.Close()
		return nil, err
	}
	m.w = bufio.NewWriter(f)
	return m, nil
}

// replay walks the log, applying every complete record to v. It
// returns the offset after the last good record, the next file ID, and
// the persisted sequence watermark.
func replay(f *os.File, v *Version) (int64, uint64, uint64, error) {
	var hdr [8]byte
	var off int64
	next := uint64(1)
	nextSeq := uint64(0)
	bump := func(id uint64) {
		if id >= next {
			next = id + 1
		}
	}
	for {
		if _, err := f.ReadAt(hdr[:], off); err != nil {
			// EOF at any point of the header is a clean or torn
			// end; either way the good prefix ends at off.
			return off, next, nextSeq, nil
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		want := binary.LittleEndian.Uint32(hdr[4:8])
		if n > 1<<24 {
			return off, next, nextSeq, nil
		}
		payload := make([]byte, n)
		if _, err := f.ReadAt(payload, off+8); err != nil {
			return off, next, nextSeq, nil
		}
		if crc32.Checksum(payload, castagnoli) != want {
			return off, next, nextSeq, nil
		}
		val, kind, err := decodeRecord(payload, v)
		if err != nil {
			return off, next, nextSeq, nil
		}
		switch kind {
		case markNextID:
			if val > next {
				next = val
			}
		case markNextSeq:
			if val > nextSeq {
				nextSeq = val
			}
		default:
			bump(val)
		}
		off += 8 + int64(n)
	}
}

// Where a decoded record's value goes.
const (
	markFileID = iota
	markNextID
	markNextSeq
)

// decodeRecord applies one record and reports the value it carried and
// which counter it belongs to.
func decodeRecord(p []byte, v *Version) (uint64, int, error) {
	if len(p) == 0 {
		return 0, markFileID, errors.New("empty record")
	}
	tag, p := p[0], p[1:]
	switch tag {
	case recAdd:
		m := TableMeta{}
		var ok bool
		if m.ID, p, ok = getUvarint(p); !ok {
			return 0, markFileID, errors.New("bad add record")
		}
		var lvl uint64
		if lvl, p, ok = getUvarint(p); !ok {
			return 0, markFileID, errors.New("bad add record")
		}
		m.Level = int(lvl)
		if m.Size, p, ok = getUvarint(p); !ok {
			return 0, markFileID, errors.New("bad add record")
		}
		if m.Count, p, ok = getUvarint(p); !ok {
			return 0, markFileID, errors.New("bad add record")
		}
		if m.Smallest, p, ok = getBytes(p); !ok {
			return 0, markFileID, errors.New("bad add record")
		}
		if m.Largest, p, ok = getBytes(p); !ok {
			return 0, markFileID, errors.New("bad add record")
		}
		v.ensureLevels(m.Level)
		v.Levels[m.Level] = append(v.Levels[m.Level], m)
		return m.ID, markFileID, nil
	case recDelete:
		id, _, ok := getUvarint(p)
		if !ok {
			return 0, markFileID, errors.New("bad delete record")
		}
		del := &Edit{Deleted: []uint64{id}}
		*v = *apply(v, del)
		return id, markFileID, nil
	case recNextID:
		id, _, ok := getUvarint(p)
		if !ok {
			return 0, markNextID, errors.New("bad next-id record")
		}
		return id, markNextID, nil
	case recNextSeq:
		seq, _, ok := getUvarint(p)
		if !ok {
			return 0, markNextSeq, errors.New("bad next-seq record")
		}
		return seq, markNextSeq, nil
	default:
		return 0, markFileID, errors.New("unknown record tag")
	}
}

func getUvarint(p []byte) (uint64, []byte, bool) {
	x, n := binary.Uvarint(p)
	if n <= 0 {
		return 0, p, false
	}
	return x, p[n:], true
}

func getBytes(p []byte) ([]byte, []byte, bool) {
	n, p, ok := getUvarint(p)
	if !ok || n > uint64(len(p)) {
		return nil, p, false
	}
	return p[:n], p[n:], true
}
