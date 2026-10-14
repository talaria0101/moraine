// Package block implements the on-disk block format shared by data
// blocks and index blocks. Entries are prefix compressed against the
// previous entry, with a restart point every RestartInterval entries so
// that a seek only has to decompress from the nearest restart.
//
// Layout:
//
//	entry*                       varint shared, varint unshared, key
//	                             delta, varint value length, value
//	restart* (uint32 LE)         block offsets of restart entries
//	numRestarts (uint32 LE)
//	crc32c (uint32 LE)           checksum of everything above
//
// Restart entries are always written with a shared prefix of zero, so a
// seek can binary search restarts without reconstructing earlier keys.
package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// RestartInterval is the number of entries between restart points. It
// bounds the linear scan after the restart binary search.
const RestartInterval = 16

// trailerLen is the restart count field plus the checksum field.
const trailerLen = 8

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt is returned when a block fails its checksum or is malformed.
var ErrCorrupt = errors.New("moraine: corrupt block")

// Builder assembles a block from entries added in strictly increasing
// key order.
type Builder struct {
	buf          []byte
	restarts     []uint32
	prevKey      []byte
	sinceRestart int
}

// NewBuilder returns an empty builder.
func NewBuilder() *Builder { return &Builder{} }

// Empty reports whether no entry has been added.
func (b *Builder) Empty() bool { return len(b.buf) == 0 }

// size returns the current encoded entry size without the restart
// trailer, current byte length, used to decide when to close a block.
func (b *Builder) Len() int { return len(b.buf) }

// Add appends one entry. Keys must arrive in strictly increasing order
// across the life of the builder; Add panics otherwise, because an
// out-of-order table would silently lose entries.
func (b *Builder) Add(key, value []byte) {
	if b.sinceRestart > 0 && bytes.Compare(key, b.prevKey) <= 0 {
		panic(fmt.Sprintf("block: out of order key %q after %q", key, b.prevKey))
	}
	shared := 0
	if b.sinceRestart > 0 {
		n := min(len(key), len(b.prevKey))
		for shared < n && key[shared] == b.prevKey[shared] {
			shared++
		}
	}
	start := uint32(len(b.buf))
	if b.sinceRestart == 0 {
		b.restarts = append(b.restarts, start)
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	unshared := key[shared:]
	b.buf = binary.AppendUvarint(b.buf, uint64(len(unshared)))
	b.buf = append(b.buf, unshared...)
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, value...)

	b.prevKey = append(b.prevKey[:0], key...)
	b.sinceRestart++
	if b.sinceRestart >= RestartInterval {
		b.sinceRestart = 0
	}
}

// Finish appends the restart table and checksum, and returns the
// finished block. The builder must not be used afterwards.
func (b *Builder) Finish() []byte {
	for _, r := range b.restarts {
		b.buf = binary.LittleEndian.AppendUint32(b.buf, r)
	}
	b.buf = binary.LittleEndian.AppendUint32(b.buf, uint32(len(b.restarts)))
	b.buf = binary.LittleEndian.AppendUint32(b.buf, crc32.Checksum(b.buf, castagnoli))
	out := b.buf
	b.buf = nil
	b.restarts = nil
	return out
}

// Entry is one decoded key/value pair. Key and value alias iterator
// owned buffers and are only valid until the next iterator call.
type Entry struct {
	Key   []byte
	Value []byte
}

// Iter iterates a finished block. The block slice must stay valid for
// the life of the iterator. An Iter is not safe for concurrent use.
type Iter struct {
	data     []byte // entries region, restart table excluded
	restarts []uint32
	keyBuf   []byte // reconstructed current key
	cur      Entry
	pos      int
	valid    bool
	err      error
}

// NewIter validates the block trailer and returns an iterator positioned
// before the first entry. It returns ErrCorrupt on a bad checksum or a
// truncated block.
func NewIter(raw []byte) (*Iter, error) {
	if len(raw) < trailerLen {
		return nil, fmt.Errorf("%w: block len %d", ErrCorrupt, len(raw))
	}
	body := raw[:len(raw)-4]
	sum := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(raw[len(raw)-8:])
	if n < 1 {
		return nil, fmt.Errorf("%w: empty block", ErrCorrupt)
	}
	tableStart := len(raw) - trailerLen - int(n)*4
	if tableStart < 0 {
		return nil, fmt.Errorf("%w: restart table overruns block", ErrCorrupt)
	}
	restarts := make([]uint32, n)
	for i := range restarts {
		off := binary.LittleEndian.Uint32(raw[tableStart+i*4:])
		if int(off) > tableStart {
			return nil, fmt.Errorf("%w: restart offset %d beyond entries", ErrCorrupt, off)
		}
		restarts[i] = off
	}
	if restarts[0] != 0 {
		return nil, fmt.Errorf("%w: first restart offset %d", ErrCorrupt, restarts[0])
	}
	return &Iter{data: raw[:tableStart], restarts: restarts}, nil
}

// Error returns the first decoding error hit, if any.
func (it *Iter) Error() error { return it.err }

// Valid reports whether the iterator is positioned on an entry.
func (it *Iter) Valid() bool { return it.valid }

// Current returns the entry under the cursor. It is only meaningful
// when Valid is true.
func (it *Iter) Current() Entry { return it.cur }

// decode reads the entry at offset off. The returned key is copied into
// it.keyBuf, the returned value aliases it.data.
func (it *Iter) decode(off int) (Entry, int, bool) {
	d := it.data
	getUvarint := func() uint64 {
		v, n := binary.Uvarint(d[off:])
		if n <= 0 {
			it.err = fmt.Errorf("%w: bad varint at offset %d", ErrCorrupt, off)
			return 0
		}
		off += n
		return v
	}
	fail := func(reason string) (Entry, int, bool) {
		it.err = fmt.Errorf("%w: %s at offset %d", ErrCorrupt, reason, off)
		return Entry{}, 0, false
	}
	shared := getUvarint()
	if it.err != nil {
		return fail("varint")
	}
	ulen := getUvarint()
	if it.err != nil {
		return fail("varint")
	}
	if uint64(shared) > uint64(len(it.keyBuf)) {
		return fail("shared prefix longer than previous key")
	}
	if off+int(ulen) > len(d) {
		return fail("key overruns block")
	}
	keyStart := off
	off += int(ulen)
	if shared == 0 {
		it.keyBuf = append(it.keyBuf[:0], d[keyStart:off]...)
	} else {
		it.keyBuf = append(it.keyBuf[:shared], d[keyStart:off]...)
	}
	vlen := getUvarint()
	if it.err != nil {
		return fail("varint")
	}
	if off+int(vlen) > len(d) {
		return fail("value overruns block")
	}
	value := d[off : off+int(vlen)]
	off += int(vlen)
	return Entry{Key: it.keyBuf, Value: value}, off, true
}

// load decodes the entry at it.pos and updates the cursor. Reaching the
// end of the entries region is a normal stop, not an error.
func (it *Iter) load() {
	if it.pos >= len(it.data) {
		it.pos = len(it.data)
		it.valid = false
		return
	}
	e, next, ok := it.decode(it.pos)
	if !ok {
		it.pos = len(it.data)
		it.valid = false
		return
	}
	it.cur = e
	it.pos = next
	it.valid = true
}

// First positions on the first entry and reports success.
func (it *Iter) First() bool {
	it.err = nil
	it.pos = int(it.restarts[0])
	it.load()
	return it.valid
}

// Next advances to the next entry and reports success.
func (it *Iter) Next() bool {
	if !it.valid {
		return false
	}
	it.load()
	return it.valid
}

// restartKey decodes the key of restart point i without touching the
// cursor or keyBuf. Restart entries have a zero shared prefix.
func (it *Iter) restartKey(i int) []byte {
	off := int(it.restarts[i])
	_, n1 := binary.Uvarint(it.data[off:]) // shared, always 0
	ulen, n2 := binary.Uvarint(it.data[off+n1:])
	return it.data[off+n1+n2 : off+n1+n2+int(ulen)]
}

// Seek positions the cursor on the first entry with key >= target and
// reports whether such an entry exists.
func (it *Iter) Seek(target []byte) bool {
	it.err = nil
	// Largest restart whose key is <= target. Binary search over
	// restart points only, then scan forward.
	lo, hi := 0, len(it.restarts)-1
	for lo < hi {
		mid := int(uint(lo+hi+1) / 2)
		if bytes.Compare(it.restartKey(mid), target) <= 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	it.pos = int(it.restarts[lo])
	it.valid = false
	for {
		e, next, ok := it.decode(it.pos)
		if !ok {
			it.pos = len(it.data)
			return false
		}
		if bytes.Compare(e.Key, target) >= 0 {
			it.cur = e
			it.pos = next
			it.valid = true
			return true
		}
		it.pos = next
		if it.pos >= len(it.data) {
			return false
		}
	}
}
