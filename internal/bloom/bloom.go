// Package bloom implements a space-efficient probabilistic membership
// filter over byte strings, used by sstables to skip point lookups that
// cannot be present. The false positive rate for m bits per key with
// k probes is approximately 0.6185^m; k is derived from m by k =
// round(m * ln 2), the classic optimum from Bloom (1970).
package bloom

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrCorrupt is returned by Decode on a malformed payload.
var ErrCorrupt = errors.New("moraine: corrupt bloom filter")

// Hash is the 64-bit FNV-1a hash. It is fast, has no dependencies, and
// is good enough to drive double hashing; cryptographic strength is
// irrelevant for filters whose false positives cost one block read.
func Hash(b []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

// Filter is a finished bloom filter. A nil *Filter answers "maybe" to
// everything, which is the shape the sstable reader wants when bloom
// filters are disabled.
type Filter struct {
	bits      []byte
	numProbes uint32
}

// New builds a filter over keys with bitsPerKey bits per key. It
// returns nil for bitsPerKey <= 0, meaning "no filter".
func New(keys [][]byte, bitsPerKey int) *Filter {
	if bitsPerKey <= 0 {
		return nil
	}
	n := len(keys)
	bits := (n*bitsPerKey + 7) / 8
	if bits < 8 {
		bits = 8
	}
	k := uint32(math.Ln2 * float64(bitsPerKey))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	f := &Filter{bits: make([]byte, bits), numProbes: k}
	for _, key := range keys {
		f.add(key)
	}
	return f
}

// doubleHash returns the probe step derived from the base hash.
func doubleHash(h uint64) uint64 {
	return h>>17 | h<<47
}

func (f *Filter) add(key []byte) {
	h := Hash(key)
	delta := doubleHash(h)
	nbits := uint64(len(f.bits)) * 8
	for i := uint32(0); i < f.numProbes; i++ {
		bit := h % nbits
		f.bits[bit/8] |= 1 << (bit % 8)
		h += delta
	}
}

// MayContain reports whether key might be in the filter. False
// positives are possible; false negatives are not, for keys that were
// added.
func (f *Filter) MayContain(key []byte) bool {
	if f == nil {
		return true
	}
	h := Hash(key)
	delta := doubleHash(h)
	nbits := uint64(len(f.bits)) * 8
	for i := uint32(0); i < f.numProbes; i++ {
		bit := h % nbits
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// Encode appends the wire format to dst: numProbes (uint32 LE), byte
// length (uint32 LE), bits.
func (f *Filter) Encode(dst []byte) []byte {
	if f == nil {
		return dst
	}
	dst = binary.LittleEndian.AppendUint32(dst, f.numProbes)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(f.bits)))
	return append(dst, f.bits...)
}

// Decode parses a filter from src. It returns (nil, nil) for an empty
// slice, meaning "no filter present".
func Decode(src []byte) (*Filter, error) {
	if len(src) == 0 {
		return nil, nil
	}
	if len(src) < 8 {
		return nil, ErrCorrupt
	}
	k := binary.LittleEndian.Uint32(src)
	n := binary.LittleEndian.Uint32(src[4:])
	if uint64(n)+8 != uint64(len(src)) || k < 1 || k > 30 {
		return nil, ErrCorrupt
	}
	return &Filter{bits: src[8:], numProbes: k}, nil
}

// Len returns the encoded byte length.
func (f *Filter) Len() int {
	if f == nil {
		return 0
	}
	return 8 + len(f.bits)
}
