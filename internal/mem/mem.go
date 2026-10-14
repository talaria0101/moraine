// Package mem implements the memtable: a skip list holding recent
// writes in internal key order until they are flushed into an sstable.
//
// The skip list uses the classic Pugh (1990) structure with probability
// 1/4 and 12 levels, which keeps expected comparisons per insert around
// 1/(p ln(1/p)) = 7.2 and bounds worst case memory per node.
package mem

import (
	"sync"
	"sync/atomic"

	"github.com/talaria0101/moraine/internal/key"
)

const (
	maxHeight = 12
	// branchBits: each level descends when the low 2 bits are zero,
	// i.e. branching probability 1/4.
	branchBits = 3
)

type node struct {
	ikey   []byte // encoded internal key
	value  []byte
	tower  [maxHeight]*node
	height int
}

// Table is a skip list memtable. Writers take an exclusive lock; point
// reads take a shared lock. Iteration happens only on frozen tables,
// which by DB policy receive no further writes, so iterators take no
// lock.
type Table struct {
	head *node
	rng  atomic.Uint64

	mu   sync.RWMutex
	size int64 // approximate encoded bytes
	len  int
}

// New returns an empty memtable.
func New() *Table {
	return &Table{head: &node{height: maxHeight}}
}

func (t *Table) randHeight() int {
	// splitmix64 finalizer over a wrapping counter: cheap, adequate,
	// and deterministic per stream position which helps reproducible
	// tests.
	x := t.rng.Add(0x9e3779b97f4a7c15)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	h := 1
	for h < maxHeight && x&branchBits == 0 {
		h++
		x >>= 2
	}
	return h
}

// Put inserts one entry. The key must carry a sequence number already
// assigned by the DB. Keys and values are copied.
func (t *Table) Put(ikey, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	k := append([]byte(nil), ikey...)
	v := append([]byte(nil), value...)
	h := t.randHeight()
	n := &node{ikey: k, value: v, height: h}

	var prev [maxHeight]*node
	cur := t.head
	for level := maxHeight - 1; level >= 0; level-- {
		for cur.tower[level] != nil && key.Compare(cur.tower[level].ikey, k) < 0 {
			cur = cur.tower[level]
		}
		prev[level] = cur
	}
	for level := 0; level < h; level++ {
		n.tower[level] = prev[level].tower[level]
		prev[level].tower[level] = n
	}
	t.size += int64(len(k) + len(v) + 64)
	t.len++
}

// Get returns the newest entry for userKey with sequence <= snap.
func (t *Table) Get(userKey []byte, snap uint64) (key.Internal, []byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	target := targetFor(userKey)
	cur := t.head
	for level := maxHeight - 1; level >= 0; level-- {
		for cur.tower[level] != nil && key.Compare(cur.tower[level].ikey, target) < 0 {
			cur = cur.tower[level]
		}
	}
	cur = cur.tower[0]
	for cur != nil {
		ik, ok := key.Decode(cur.ikey)
		if !ok {
			return key.Internal{}, nil, false
		}
		c := key.CompareUser(ik.UserKey, userKey)
		if c > 0 {
			break
		}
		if c == 0 && ik.Seq <= snap {
			return ik, cur.value, true
		}
		cur = cur.tower[0]
	}
	return key.Internal{}, nil, false
}

// targetFor is the smallest internal key of userKey.
func targetFor(userKey []byte) []byte {
	return key.Encode(nil, userKey, key.MaxSeq, key.TypeDelete)
}

// Size returns the approximate byte size including per node overhead.
func (t *Table) Size() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.size
}

// Len returns the number of entries.
func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.len
}

// Iter walks the memtable in internal key order. Not safe for
// concurrent use with writes.
type Iter struct {
	t   *Table
	cur *node
}

// NewIter returns an iterator positioned before the first entry. The
// memtable must not receive writes while the iterator is used, which
// the DB guarantees by freezing a table before anyone iterates it.
func (t *Table) NewIter() *Iter {
	return &Iter{t: t, cur: t.head}
}

// First positions on the first entry.
func (it *Iter) First() bool {
	it.cur = it.t.head.tower[0]
	return it.cur != nil
}

// Valid reports whether the iterator is positioned on an entry.
func (it *Iter) Valid() bool { return it.cur != nil }

// Next advances.
func (it *Iter) Next() bool {
	if it.cur == nil {
		return false
	}
	it.cur = it.cur.tower[0]
	return it.cur != nil
}

// Current returns the entry under the cursor; the slices alias the
// memtable and stay valid until the memtable is released.
func (it *Iter) Current() (key.Internal, []byte) {
	ik, _ := key.Decode(it.cur.ikey)
	return ik, it.cur.value
}

// Seek positions on the first entry >= target.
func (it *Iter) Seek(target []byte) bool {
	cur := it.t.head
	for level := maxHeight - 1; level >= 0; level-- {
		for cur.tower[level] != nil && key.Compare(cur.tower[level].ikey, target) < 0 {
			cur = cur.tower[level]
		}
	}
	it.cur = cur.tower[0]
	return it.cur != nil
}
