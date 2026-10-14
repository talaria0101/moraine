// Package db wires the primitives into a working LSM-tree KV engine:
// memtable plus WAL, background flushes to L0, leveled-style
// compaction between levels, snapshot reads and merge scans.
package db

// Options configures a database. Zero fields take the documented
// default.
type Options struct {
	// WriteBuffer is the memtable size at which the DB rotates to a
	// fresh memtable and flushes the old one. Default 4 MiB.
	WriteBuffer int
	// BlockSize is the sstable block size. Default 16 KiB.
	BlockSize int
	// BitsPerKey is the bloom filter size in bits per key. 0 means
	// the table default (10); negative disables filters.
	BitsPerKey int
	// Sync fsyncs the WAL on every write when true. Durability point.
	Sync bool
	// TargetFileSize splits compaction output into files of about
	// this many bytes, never mid-user-key. Default 2 MiB.
	TargetFileSize uint64
	// L0CompactionTrigger fires an L0-to-L1 compaction once L0 holds
	// this many files. Default 4.
	L0CompactionTrigger int
	// MaxLevels is the number of levels including L0. Default 7.
	MaxLevels int
	// L1TargetBytes is the size target of L1; level l's target is
	// L1TargetBytes times LevelMultiplier to the (l-1). Default
	// 8 MiB.
	L1TargetBytes uint64
	// LevelMultiplier scales per-level size targets. Default 10.
	LevelMultiplier uint64
}

func (o *Options) fill() {
	if o.WriteBuffer <= 0 {
		o.WriteBuffer = 4 << 20
	}
	if o.BlockSize <= 0 {
		o.BlockSize = 16 << 10
	}
	if o.BitsPerKey == 0 {
		o.BitsPerKey = 10
	}
	if o.TargetFileSize <= 0 {
		o.TargetFileSize = 2 << 20
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = 4
	}
	if o.MaxLevels <= 0 {
		o.MaxLevels = 7
	}
	if o.L1TargetBytes <= 0 {
		o.L1TargetBytes = 8 << 20
	}
	if o.LevelMultiplier <= 1 {
		o.LevelMultiplier = 10
	}
}

// levelTarget returns the byte size target of level l (l >= 1).
func (o *Options) levelTarget(l int) uint64 {
	t := o.L1TargetBytes
	for i := 1; i < l; i++ {
		t *= o.LevelMultiplier
	}
	return t
}
