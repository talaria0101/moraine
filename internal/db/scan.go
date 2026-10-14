package db

import (
	"github.com/talaria0101/moraine/internal/key"
)

// rawIter is the merged-iterator view of one source: memtable or
// table. Positioning returns raw internal keys so the merge compares
// through key.Compare directly.
type rawIter interface {
	First() bool
	Next() bool
	Valid() bool
	CurrentRaw() ([]byte, []byte)
	Error() error
}

type mergeSrc struct {
	it  rawIter
	ik  []byte
	val []byte
	ok  bool
}

func (s *mergeSrc) prime(first bool) {
	if first {
		s.ok = s.it.First()
	} else {
		s.ok = s.it.Next()
	}
	if s.ok {
		s.ik, s.val = s.it.CurrentRaw()
	} else {
		s.ik, s.val = nil, nil
	}
}

// DBIter merges every source into one stream of user keys. For each
// user key it surfaces the newest version visible at the snapshot: a
// set surfaces key and value; a delete surfaces nothing and the user
// key does not appear. Sources must be supplied newest first.
//
// The source that produced the current entry stays parked on it; the
// next positioning call advances it. That keeps First and Seek able
// to land on a key again instead of skipping the one in hand.
type DBIter struct {
	snap   uint64
	srcs   []*mergeSrc
	lastU  []byte // user key emitted last, nil at start
	curSrc int    // source that produced the current entry, -1 if none
	valid  bool

	ukey []byte
	val  []byte
	typ  key.Type
	err  error

	pin   []flushJob // pins immutable memtables against GC until Close
	close []func() error
}

// NewIter returns an iterator over the current state at the current
// sequence. Immutable memtables pinned by the iterator stay alive
// until Close even if a flush finishes mid-scan.
func (db *DB) NewIter() (*DBIter, error) {
	return db.NewIterAt(db.seq.Load())
}

// NewIterAt returns an iterator reading at an explicit snapshot.
func (db *DB) NewIterAt(snap uint64) (*DBIter, error) {
	if err := db.bgFailure(); err != nil {
		return nil, err
	}
	v := db.man.Current()
	db.mu.Lock()
	memT := db.mem
	imms := append([]flushJob(nil), db.imm...)
	db.mu.Unlock()

	it := &DBIter{snap: snap, curSrc: -1}
	// Newest first: active memtable, immutables newest to oldest,
	// L0 newest to oldest, deeper levels in key order.
	it.addSrc(memT.NewIter())
	for i := len(imms) - 1; i >= 0; i-- {
		it.addSrc(imms[i].t.NewIter())
	}
	if len(v.Levels) > 0 {
		for _, m := range v.Levels[0] {
			r, release, err := db.acquireReader(m.ID)
			if err != nil {
				it.Close()
				return nil, err
			}
			it.close = append(it.close, func() error { release(); return nil })
			it.addSrc(r.NewIter())
		}
	}
	for l := 1; l < len(v.Levels); l++ {
		for _, m := range v.Levels[l] {
			r, release, err := db.acquireReader(m.ID)
			if err != nil {
				it.Close()
				return nil, err
			}
			it.close = append(it.close, func() error { release(); return nil })
			it.addSrc(r.NewIter())
		}
	}
	it.pin = imms
	it.first()
	return it, nil
}

func (it *DBIter) addSrc(raw rawIter) {
	s := &mergeSrc{it: raw}
	s.prime(true)
	it.srcs = append(it.srcs, s)
}

// pick returns the source with the smallest current internal key;
// ties go to the newest source (lowest index).
func (it *DBIter) pick() int {
	best := -1
	for i, s := range it.srcs {
		if !s.ok {
			continue
		}
		if best < 0 || key.Compare(s.ik, it.srcs[best].ik) < 0 {
			best = i
		}
	}
	return best
}

// First restarts from the beginning: every source is re-primed to
// its first key, so repeated First calls land on the same entry.
func (it *DBIter) First() bool {
	it.valid = false
	it.err = nil
	it.lastU = nil
	it.curSrc = -1
	for _, s := range it.srcs {
		s.prime(true)
	}
	it.first()
	return it.valid
}

// first positions on the first visible user key from the sources'
// current positions.
func (it *DBIter) first() {
	for {
		i := it.pick()
		if i < 0 {
			return
		}
		if it.step(i) {
			return
		}
		if it.err != nil {
			return
		}
	}
}

// releaseCurrent advances the source that produced the current entry
// past the emitted key. Positioning calls begin with this so the
// emitted entry is examined exactly once per restart.
func (it *DBIter) releaseCurrent() {
	if it.curSrc >= 0 {
		it.srcs[it.curSrc].prime(false)
		it.curSrc = -1
	}
}

// step examines the source with the smallest key; it returns true
// when a user key became current, false when scanning must continue.
// On false the source is advanced; on true it stays parked.
func (it *DBIter) step(i int) bool {
	s := it.srcs[i]
	ik, ok := key.Decode(s.ik)
	if !ok {
		it.err = errBadKey
		s.prime(false)
		return false
	}
	if ik.Seq > it.snap {
		s.prime(false) // invisible version, newer than the snapshot
		return false
	}
	if it.lastU != nil && key.CompareUser(it.lastU, ik.UserKey) == 0 {
		s.prime(false) // older version of the user key just emitted
		return false
	}
	if ik.Type != key.TypeSet {
		it.lastU = append(it.lastU[:0], ik.UserKey...)
		s.prime(false) // tombstone shadows the user key
		return false
	}
	it.lastU = append(it.lastU[:0], ik.UserKey...)
	it.curSrc = i
	it.ukey = append(it.ukey[:0], ik.UserKey...)
	it.val = append(it.val[:0], s.val...)
	it.typ = ik.Type
	it.valid = true
	return true
}

// Valid reports whether the iterator is positioned on a live entry.
func (it *DBIter) Valid() bool { return it.valid }

// UserKey returns the current user key; it aliases an internal buffer
// valid until the next positioning call.
func (it *DBIter) UserKey() []byte { return it.ukey }

// Value returns the current value; it is a copy owned by the caller's
// iterator, stable until the next positioning call.
func (it *DBIter) Value() []byte { return it.val }

// Next advances to the next visible user key.
func (it *DBIter) Next() bool {
	it.valid = false
	it.releaseCurrent()
	for {
		i := it.pick()
		if i < 0 {
			return false
		}
		if it.step(i) {
			return true
		}
		if it.err != nil {
			return false
		}
	}
}

// Seek positions on the first user key >= userKey, under the
// snapshot. It returns false when no such key exists; the iterator is
// then invalid, but it can still be Seeked or Firsted afterwards.
func (it *DBIter) Seek(userKey []byte) bool {
	it.valid = false
	it.err = nil
	it.curSrc = -1
	target := key.Encode(nil, userKey, key.MaxSeq, key.TypeDelete)
	for _, s := range it.srcs {
		if seeker, ok := s.it.(interface{ Seek([]byte) bool }); ok {
			s.ok = seeker.Seek(target)
		} else {
			s.prime(true)
			for s.ok && key.Compare(s.ik, target) < 0 {
				s.prime(false)
			}
		}
		if s.ok {
			s.ik, s.val = s.it.CurrentRaw()
		}
	}
	it.lastU = nil
	it.first()
	return it.valid
}

// Error returns the first error hit during iteration.
func (it *DBIter) Error() error { return it.err }

// Close releases pinned memtables and table readers.
func (it *DBIter) Close() error {
	for _, f := range it.close {
		f()
	}
	it.close = nil
	return nil
}

type badKeyError struct{}

func (badKeyError) Error() string { return "moraine: malformed internal key during scan" }

var errBadKey = badKeyError{}
