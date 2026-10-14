// Package key implements the internal key encoding and comparator used
// throughout moraine. Every entry is stored under an internal key that
// pairs a user key with a sequence number and a value type, so that
// multiple versions of the same user key can coexist in a table and the
// newest one wins everywhere the engine merges streams.
package key

import (
	"bytes"
	"encoding/binary"
)

// Type marks whether an entry carries a value or a deletion.
type Type uint8

const (
	// TypeDelete is a tombstone.
	TypeDelete Type = 0
	// TypeSet is a live value.
	TypeSet Type = 1
)

// MaxSeq is the largest sequence number. Sequence numbers are stored in
// the high 56 bits of the internal trailer; the low 8 bits hold the type.
const MaxSeq uint64 = 1<<56 - 1

// SeqBits is the number of bits used for the sequence number.
const SeqBits = 56

// TrailerLen is the number of bytes appended to a user key to form an
// internal key.
const TrailerLen = 8

// Encode appends an internal key (user key plus trailer) to dst and
// returns the extended slice. The trailer stores (MaxSeq - seq) in the
// high bits and the type in the low byte, so the raw byte strings of
// two internal keys order exactly like Compare says: user key ascending,
// then newest sequence first. This lets block code compare raw bytes.
func Encode(dst []byte, userKey []byte, seq uint64, typ Type) []byte {
	dst = append(dst, userKey...)
	var trailer [8]byte
	binary.BigEndian.PutUint64(trailer[:], (MaxSeq-seq)<<8|uint64(typ))
	return append(dst, trailer[:]...)
}

// Internal is a decoded internal key. UserKey aliases the underlying
// buffer, which the caller must not modify.
type Internal struct {
	UserKey []byte
	Seq     uint64
	Type    Type
}

// Decode splits an internal key. It returns false when the slice is too
// short to hold a trailer.
func Decode(b []byte) (Internal, bool) {
	if len(b) < TrailerLen {
		return Internal{}, false
	}
	t := binary.BigEndian.Uint64(b[len(b)-TrailerLen:])
	return Internal{
		UserKey: b[:len(b)-TrailerLen],
		Seq:     MaxSeq - (t >> 8),
		Type:    Type(t & 0xff),
	}, true
}

// Clone returns a copy of the internal key with its own storage.
func (k Internal) Clone() Internal {
	uk := make([]byte, len(k.UserKey))
	copy(uk, k.UserKey)
	k.UserKey = uk
	return k
}

// CompareUser compares two user keys bytewise, shortest is smaller.
func CompareUser(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}

// Compare orders internal keys: user key ascending, then stored
// trailer ascending. The stored trailer is (MaxSeq - seq) << 8 | type,
// so trailer ascending means newer sequences first. This is NOT raw
// byte order: with the inverted trailer, a user key that is a prefix of
// another would interleave wrongly if compared bytewise, so every
// ordered structure must compare through this function.
func Compare(a, b []byte) int {
	if c := CompareUser(a[:len(a)-TrailerLen], b[:len(b)-TrailerLen]); c != 0 {
		return c
	}
	return bytes.Compare(a[len(a)-TrailerLen:], b[len(b)-TrailerLen:])
}
