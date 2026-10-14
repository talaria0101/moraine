// Package wal implements the write ahead log: an append only file of
// checksummed records, one batch per record. On recovery, a torn tail
// (partial header, short payload, or checksum mismatch) stops the
// replay cleanly, which is what makes a crash between the write call
// and the fsync safe: every record either survives whole or not at all.
//
// Record layout:
//
//	uint32 LE payload length
//	uint32 LE crc32c of payload
//	payload
//
// The payload is one encoded batch (see Batch encoding).
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/talaria0101/moraine/internal/key"
)

// MaxBatchBytes bounds a batch payload so a record never needs
// fragmentation. Larger batches are rejected rather than split, which
// keeps the recovery path simple and honest.
const MaxBatchBytes = 4 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt marks a malformed record found before the torn tail.
var ErrCorrupt = errors.New("moraine: corrupt wal record")

// Entry is one mutation inside a batch.
type Entry struct {
	Key   []byte // user key
	Value []byte
	Type  key.Type
}

// Batch is an atomic group of mutations sharing one sequence base.
type Batch struct {
	Entries []Entry
	Seq     uint64 // base sequence number, assigned by the DB
}

// Encode appends the batch payload to dst.
//
//	uvarint entry count
//	uvarint base sequence
//	per entry: type byte, uvarint key len, key, uvarint value len, value
func (b *Batch) Encode(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b.Entries)))
	dst = binary.AppendUvarint(dst, b.Seq)
	for _, e := range b.Entries {
		dst = append(dst, byte(e.Type))
		dst = binary.AppendUvarint(dst, uint64(len(e.Key)))
		dst = append(dst, e.Key...)
		dst = binary.AppendUvarint(dst, uint64(len(e.Value)))
		dst = append(dst, e.Value...)
	}
	return dst
}

// decodeBatch parses a payload produced by Encode.
func decodeBatch(payload []byte) (*Batch, error) {
	b := &Batch{}
	s := payload
	u := func() (uint64, bool) {
		v, n := binary.Uvarint(s)
		if n <= 0 {
			return 0, false
		}
		s = s[n:]
		return v, true
	}
	count, ok := u()
	if !ok {
		return nil, fmt.Errorf("%w: batch count", ErrCorrupt)
	}
	seq, ok := u()
	if !ok {
		return nil, fmt.Errorf("%w: batch seq", ErrCorrupt)
	}
	b.Seq = seq
	if count > uint64(len(s)) { // every entry needs >= 3 bytes
		return nil, fmt.Errorf("%w: batch count overruns payload", ErrCorrupt)
	}
	for i := uint64(0); i < count; i++ {
		if len(s) < 1 {
			return nil, fmt.Errorf("%w: entry type", ErrCorrupt)
		}
		typ := key.Type(s[0])
		s = s[1:]
		klen, ok := u()
		if !ok || uint64(len(s)) < klen {
			return nil, fmt.Errorf("%w: entry key", ErrCorrupt)
		}
		k := s[:klen]
		s = s[klen:]
		vlen, ok := u()
		if !ok || uint64(len(s)) < vlen {
			return nil, fmt.Errorf("%w: entry value", ErrCorrupt)
		}
		v := s[:vlen]
		s = s[vlen:]
		if typ != key.TypeSet && typ != key.TypeDelete {
			return nil, fmt.Errorf("%w: entry type %d", ErrCorrupt, typ)
		}
		b.Entries = append(b.Entries, Entry{
			Key:   append([]byte(nil), k...),
			Value: append([]byte(nil), v...),
			Type:  typ,
		})
	}
	if len(s) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrCorrupt, len(s))
	}
	return b, nil
}

// Writer appends batches to a WAL file.
type Writer struct {
	f *os.File
}

// Open creates or appends to the WAL at path. Callers that never append
// after a crash should delete stale WALs instead of reopening them; the
// DB only reopens a WAL it knows is fresh.
func Open(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f}, nil
}

// Append writes one record. With sync it also fsyncs, which is the
// point where the batch becomes durable.
func (w *Writer) Append(b *Batch, sync bool) error {
	payload := b.Encode(make([]byte, 0, 64+len(b.Entries)*32))
	if len(payload) > MaxBatchBytes {
		return fmt.Errorf("moraine: batch of %d bytes exceeds %d", len(payload), MaxBatchBytes)
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.Checksum(payload, castagnoli))
	if _, err := w.f.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(payload); err != nil {
		return err
	}
	if sync {
		return w.f.Sync()
	}
	return nil
}

// Close closes the file.
func (w *Writer) Close() error { return w.f.Close() }

// Sync fsyncs the file.
func (w *Writer) Sync() error { return w.f.Sync() }

// Reader replays a WAL.
type Reader struct {
	f       *os.File
	goodEnd int64 // offset just past the last intact record
}

// OpenReader opens a WAL for replay.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{f: f}, nil
}

// Next returns the next intact batch. It returns io.EOF at a clean end
// or a torn tail; either way recovery stops and Truncate can remove the
// remainder.
func (r *Reader) Next() (*Batch, error) {
	var header [8]byte
	if _, err := io.ReadFull(r.f, header[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	length := binary.LittleEndian.Uint32(header[0:4])
	sum := binary.LittleEndian.Uint32(header[4:8])
	if length > MaxBatchBytes {
		return nil, fmt.Errorf("%w: record length %d", ErrCorrupt, length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r.f, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF // torn tail
		}
		return nil, err
	}
	if crc32.Checksum(payload, castagnoli) != sum {
		return nil, io.EOF // treat checksum failure as a torn tail
	}
	b, err := decodeBatch(payload)
	if err != nil {
		return nil, err
	}
	r.goodEnd += int64(len(header)) + int64(length)
	return b, nil
}

// GoodEnd returns the offset just past the last intact record.
func (r *Reader) GoodEnd() int64 { return r.goodEnd }

// Close closes the reader.
func (r *Reader) Close() error { return r.f.Close() }
