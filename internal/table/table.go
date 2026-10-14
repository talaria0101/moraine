// Package table implements the sorted string table (sstable) file
// format: a sequence of prefix compressed data blocks, a whole file
// bloom filter over user keys, an index mapping separator keys to block
// handles, and a fixed size footer.
//
// Layout:
//
//	data block*        entries in internal key order
//	filter block       bloom filter, optional
//	index block        separator key -> block handle
//	footer             40 bytes, fixed
//
// The footer holds the index and filter handles, a checksum, and the
// magic bytes "MRN1". Every block carries its own CRC32C, so a truncated
// or corrupted file fails at read time with ErrCorrupt rather than
// returning wrong data.
package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/talaria0101/moraine/internal/block"
	"github.com/talaria0101/moraine/internal/bloom"
	"github.com/talaria0101/moraine/internal/key"
)

// FooterLen is the fixed footer size.
const FooterLen = 40

var magic = []byte("MRN1")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt marks malformed table files.
var ErrCorrupt = errors.New("moraine: corrupt table")

// Options controls table encoding.
type Options struct {
	// BlockSize is the target uncompressed data block size. Zero means
	// 16 KiB.
	BlockSize int
	// BitsPerKey is the bloom filter size per user key. Zero means 10,
	// negative disables the filter.
	BitsPerKey int
}

func (o Options) blockSize() int {
	if o.BlockSize > 0 {
		return o.BlockSize
	}
	return 16 << 10
}

func (o Options) bitsPerKey() int {
	switch {
	case o.BitsPerKey < 0:
		return 0
	case o.BitsPerKey == 0:
		return 10
	}
	return o.BitsPerKey
}

// Meta describes a finished table.
type Meta struct {
	Smallest []byte // internal key
	Largest  []byte // internal key
	Count    uint64
	Size     uint64
}

// Handle locates a block inside a file.
type Handle struct {
	Offset uint64
	Length uint64
}

// separator is one index entry: the actual last internal key of a
// block, and the block handle. Using the real key (rather than a
// shortened separator) keeps the index correct when one user key's
// versions span multiple blocks.
type separator struct {
	sep []byte
	h   Handle
}

// Writer streams entries into a table file. Entries must be added in
// strictly increasing internal key order.
type Writer struct {
	w        io.Writer
	opts     Options
	b        *block.Builder
	pos      uint64 // bytes written so far
	count    uint64
	smallest []byte
	largest  []byte
	userKeys [][]byte
	seps     []separator
	err      error
	fin      bool
}

// NewWriter returns a writer over w.
func NewWriter(w io.Writer, opts Options) *Writer {
	return &Writer{w: w, opts: opts, b: block.NewBuilder()}
}

func (w *Writer) write(b []byte) {
	if w.err != nil {
		return
	}
	if _, err := w.w.Write(b); err != nil {
		w.err = err
		return
	}
	w.pos += uint64(len(b))
}

// Add appends one entry. Keys must arrive in strictly increasing order;
// Add panics on violations because an out-of-order table silently loses
// data downstream.
func (w *Writer) Add(ikey, value []byte) {
	if w.err != nil {
		return
	}
	if w.fin {
		panic("table: Add after Finish")
	}
	if w.largest != nil && key.Compare(ikey, w.largest) <= 0 {
		panic("table: out of order entry")
	}
	if w.smallest == nil {
		w.smallest = append([]byte(nil), ikey...)
	}
	w.b.Add(ikey, value)
	ik, _ := key.Decode(ikey)
	w.userKeys = append(w.userKeys, append([]byte(nil), ik.UserKey...))
	w.largest = append(w.largest[:0], ikey...)
	w.count++
	if w.b.Len() >= w.opts.blockSize() {
		w.flushBlock()
	}
}

func (w *Writer) flushBlock() {
	if w.b.Empty() {
		return
	}
	raw := w.b.Finish()
	start := w.pos
	w.write(raw)
	w.seps = append(w.seps, separator{
		sep: append([]byte(nil), w.largest...),
		h:   Handle{Offset: start, Length: uint64(len(raw))},
	})
	w.b = block.NewBuilder()
}

// Finish writes the filter, index, and footer, and returns the table
// metadata. The writer must not be used afterwards.
func (w *Writer) Finish() (Meta, error) {
	if w.fin {
		panic("table: double Finish")
	}
	w.flushBlock()
	if w.err != nil {
		return Meta{}, w.err
	}
	w.fin = true

	// Filter block over user keys.
	var filterRaw []byte
	if bp := w.opts.bitsPerKey(); bp > 0 {
		filterRaw = bloom.New(w.userKeys, bp).Encode(nil)
	}
	filterHandle := Handle{Offset: w.pos, Length: uint64(len(filterRaw))}
	w.write(filterRaw)

	// Index block: separator -> handle, in block format.
	ib := block.NewBuilder()
	var vbuf [binary.MaxVarintLen64 * 2]byte
	for _, s := range w.seps {
		n := binary.PutUvarint(vbuf[:], s.h.Offset)
		n += binary.PutUvarint(vbuf[n:], s.h.Length)
		ib.Add(s.sep, vbuf[:n])
	}
	indexRaw := ib.Finish()
	indexHandle := Handle{Offset: w.pos, Length: uint64(len(indexRaw))}
	w.write(indexRaw)

	// Footer.
	var footer [FooterLen]byte
	binary.LittleEndian.PutUint64(footer[0:8], indexHandle.Offset)
	binary.LittleEndian.PutUint64(footer[8:16], indexHandle.Length)
	binary.LittleEndian.PutUint64(footer[16:24], filterHandle.Offset)
	binary.LittleEndian.PutUint64(footer[24:32], filterHandle.Length)
	binary.LittleEndian.PutUint32(footer[32:36], crc32.Checksum(footer[:32], castagnoli))
	copy(footer[36:40], magic)
	w.write(footer[:])

	return Meta{
		Smallest: append([]byte(nil), w.smallest...),
		Largest:  append([]byte(nil), w.largest...),
		Count:    w.count,
		Size:     w.pos,
	}, w.err
}

// Abort releases the writer without finishing the file.
func (w *Writer) Abort() { w.fin = true }

// ReaderAt is the read side of a table file. os.File satisfies it.
type ReaderAt interface {
	ReadAt(p []byte, off int64) (int, error)
}

// Reader reads a finished table. It loads the index and filter into
// memory and reads data blocks on demand. A Reader is safe for
// concurrent use.
type Reader struct {
	r      ReaderAt
	size   uint64
	seps   []separator
	filter *bloom.Filter

	pool sync.Pool // of *[]byte, reused data block read buffers
}

// Open validates the footer and loads the index and filter.
func Open(r ReaderAt, size uint64) (*Reader, error) {
	if size < FooterLen {
		return nil, fmt.Errorf("%w: file size %d", ErrCorrupt, size)
	}
	var footer [FooterLen]byte
	if _, err := r.ReadAt(footer[:], int64(size-FooterLen)); err != nil {
		return nil, err
	}
	if !bytes.Equal(footer[36:40], magic) {
		return nil, fmt.Errorf("%w: bad magic", ErrCorrupt)
	}
	if crc32.Checksum(footer[:32], castagnoli) != binary.LittleEndian.Uint32(footer[32:36]) {
		return nil, fmt.Errorf("%w: footer checksum", ErrCorrupt)
	}
	indexHandle := Handle{
		Offset: binary.LittleEndian.Uint64(footer[0:8]),
		Length: binary.LittleEndian.Uint64(footer[8:16]),
	}
	filterHandle := Handle{
		Offset: binary.LittleEndian.Uint64(footer[16:24]),
		Length: binary.LittleEndian.Uint64(footer[24:32]),
	}
	for _, h := range []Handle{indexHandle, filterHandle} {
		if h.Offset+h.Length+FooterLen > size {
			return nil, fmt.Errorf("%w: handle beyond file", ErrCorrupt)
		}
	}
	rd := &Reader{
		r:    r,
		size: size,
		pool: sync.Pool{New: func() any { buf := make([]byte, 0, 32<<10); return &buf }},
	}
	indexRaw, err := rd.readBlockRaw(indexHandle)
	if err != nil {
		return nil, err
	}
	iter, err := block.NewIter(indexRaw)
	if err != nil {
		return nil, err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		e := iter.Current()
		off, n := binary.Uvarint(e.Value)
		if n <= 0 {
			return nil, fmt.Errorf("%w: bad index handle", ErrCorrupt)
		}
		length, n2 := binary.Uvarint(e.Value[n:])
		if n2 <= 0 {
			return nil, fmt.Errorf("%w: bad index handle", ErrCorrupt)
		}
		rd.seps = append(rd.seps, separator{
			sep: append([]byte(nil), e.Key...),
			h:   Handle{Offset: off, Length: length},
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	if filterHandle.Length > 0 {
		filterRaw, err := rd.readBlockRaw(filterHandle)
		if err != nil {
			return nil, err
		}
		rd.filter, err = bloom.Decode(filterRaw)
		if err != nil {
			return nil, err
		}
	}
	return rd, nil
}

func (r *Reader) readBlockRaw(h Handle) ([]byte, error) {
	buf := make([]byte, h.Length)
	if _, err := r.r.ReadAt(buf, int64(h.Offset)); err != nil {
		return nil, err
	}
	return buf, nil
}

// seekBlock returns the index of the first separator >= target, or
// len(seps) if none.
func (r *Reader) seekBlock(target []byte) int {
	lo, hi := 0, len(r.seps)
	for lo < hi {
		mid := int(uint(lo+hi) / 2)
		if key.Compare(r.seps[mid].sep, target) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// targetFor encodes the seek target that sorts at or before every
// version of userKey: the user key with the minimal trailer (sequence
// MaxSeq, type 0), possible because the trailer stores MaxSeq - seq.
func targetFor(userKey []byte) []byte {
	return key.Encode(nil, userKey, key.MaxSeq, key.TypeDelete)
}

// Get returns the newest visible entry (highest sequence <= snap) for
// the user key. The returned value is a copy owned by the caller.
func (r *Reader) Get(userKey []byte, snap uint64) (key.Internal, []byte, bool, error) {
	if !r.filter.MayContain(userKey) {
		return key.Internal{}, nil, false, nil
	}
	target := targetFor(userKey)
	// target is the smallest internal key of userKey, so seekBlock
	// lands on the first block that may hold its versions; versions of
	// one user key can span several blocks, and block user keys are
	// non-decreasing, so keep scanning while the next separator's user
	// key is still userKey.
	for idx := r.seekBlock(target); idx < len(r.seps); idx++ {
		entry, value, ok, err := r.getFromBlock(r.seps[idx].h, userKey, snap)
		if err != nil {
			return key.Internal{}, nil, false, err
		}
		if ok {
			return entry, value, true, nil
		}
		if idx+1 < len(r.seps) {
			nextSep, _ := key.Decode(r.seps[idx+1].sep)
			if key.CompareUser(nextSep.UserKey, userKey) != 0 {
				break
			}
		}
	}
	return key.Internal{}, nil, false, nil
}

func (r *Reader) getFromBlock(h Handle, userKey []byte, snap uint64) (key.Internal, []byte, bool, error) {
	bufp := r.pool.Get().(*[]byte)
	defer r.pool.Put(bufp)
	raw, err := r.readBlockAt(bufp, h)
	if err != nil {
		return key.Internal{}, nil, false, err
	}
	iter, err := block.NewIter(raw)
	if err != nil {
		return key.Internal{}, nil, false, err
	}
	if !iter.Seek(targetFor(userKey)) {
		return key.Internal{}, nil, false, iter.Error()
	}
	for ; iter.Valid(); iter.Next() {
		e := iter.Current()
		ik, ok := key.Decode(e.Key)
		if !ok {
			return key.Internal{}, nil, false, fmt.Errorf("%w: short key", ErrCorrupt)
		}
		if c := key.CompareUser(ik.UserKey, userKey); c > 0 {
			break
		}
		if ik.Seq <= snap {
			// entries sort newest first; the first visible is newest
			return ik.Clone(), append([]byte(nil), e.Value...), true, iter.Error()
		}
	}
	return key.Internal{}, nil, false, iter.Error()
}

// readBlockAt reads block h into buf (growing it as needed) and returns
// the used prefix.
func (r *Reader) readBlockAt(buf *[]byte, h Handle) ([]byte, error) {
	if uint64(cap(*buf)) < h.Length {
		*buf = make([]byte, h.Length)
	}
	b := (*buf)[:h.Length]
	if _, err := r.r.ReadAt(b, int64(h.Offset)); err != nil {
		return nil, err
	}
	return b, nil
}

// Iter iterates all entries of a table in order. It is forward only;
// the engine's merge iterators never walk backwards. Not safe for
// concurrent use.
type Iter struct {
	r     *Reader
	index int
	iter  *block.Iter
	buf   []byte
	err   error
}

// NewIter returns an iterator positioned before the first entry.
func (r *Reader) NewIter() *Iter {
	return &Iter{r: r, index: -1}
}

// Error returns the first error hit.
func (it *Iter) Error() error { return it.err }

// Valid reports whether the iterator is positioned on an entry.
func (it *Iter) Valid() bool { return it.iter != nil && it.iter.Valid() }

// Current returns the entry under the cursor. The key is a private
// copy; the value aliases a block buffer owned by the iterator and
// stays valid until the next call.
func (it *Iter) Current() (key.Internal, []byte) {
	e := it.iter.Current()
	ik, _ := key.Decode(e.Key)
	return ik, e.Value
}

// loadBlock positions on the start of block idx.
func (it *Iter) loadBlock(idx int) bool {
	if idx >= len(it.r.seps) {
		it.iter = nil
		return false
	}
	h := it.r.seps[idx].h
	raw, err := it.r.readBlockAt(&it.buf, h)
	if err != nil {
		it.err = err
		it.iter = nil
		return false
	}
	bit, err := block.NewIter(raw)
	if err != nil {
		it.err = err
		it.iter = nil
		return false
	}
	it.index = idx
	it.iter = bit
	return bit.First()
}

// First positions on the first entry.
func (it *Iter) First() bool {
	it.err = nil
	return it.loadBlock(0)
}

// Next advances to the following entry.
func (it *Iter) Next() bool {
	if it.iter == nil {
		return false
	}
	if it.iter.Next() {
		return true
	}
	if err := it.iter.Error(); err != nil {
		it.err = err
		return false
	}
	return it.loadBlock(it.index + 1)
}

// Seek positions on the first entry >= target.
func (it *Iter) Seek(target []byte) bool {
	it.err = nil
	idx := it.r.seekBlock(target)
	for idx < len(it.r.seps) {
		if !it.loadBlock(idx) {
			return false
		}
		if it.iter.Seek(target) {
			return true
		}
		if err := it.iter.Error(); err != nil {
			it.err = err
			return false
		}
		// target may live in the next block: separators are the last
		// keys of their blocks.
		idx++
	}
	return false
}
