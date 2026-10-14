# Design

How moraine works, and which invariants the tests hold in place.
Paths refer to files under internal/.

## Keys and ordering

Every stored entry is an internal key: user key, 8-byte trailer
packing (MaxSeq - sequence) in the high 56 bits and the entry type
in the low 8. Sorting by user key
ascending, trailer ascending puts the NEWEST version of a user key
first, so "first entry with this user key" is "newest visible
version". Snapshots are sequence numbers: a read at snap s sees
entries with sequence <= s.

## Write path

Put/Delete append one record to the current WAL and insert into the
active skiplist. When a batch would push the memtable past
WriteBuffer bytes the DB rotates: a fresh WAL and memtable take
over, the old pair joins the flush queue. One background goroutine
flushes in order: it writes the skiplist out as an L0 sstable,
publishes the file in the manifest, then deletes the WAL. flushedTo
records the highest published sequence; "durable" means flushedTo
covers you or your WAL still exists.

A second background goroutine compacts: it scores levels under the
policy, runs one merge, publishes, repeats. Writers block while two
or more memtables sit queued for flush (the flusher broadcasts after
each), so the write path applies backpressure instead of piling up
immutable memtables unboundedly.

## Read path

Get consults, in order: active memtable, immutable memtables newest
first, every L0 file newest first, then one file per deeper level by
binary search on file ranges (FindFile). First source with a visible
entry wins; a tombstone there means absent. Each probe is counted in
Stats.SourcesProbed, so read amplification is measured, not guessed.
Bloom filters sit in front of each table probe: absent keys usually
pay one filter check per candidate file.

Scans merge all sources through one heap (DBIter). The iterator
keeps every source parked on its last emitted entry and advances
lazily; First re-primes all sources, so iteration is restartable.

## Recovery

Open replays every *.log in ascending file-number order (later files
carry later sequences), inserts the entries into a fresh memtable,
flushes it as one L0 table, and drops the WALs. The manifest
persists two watermarks: NextID (file numbers never regress) and
NextSeq (recovered WAL entries never reuse a sequence already in a
table). A torn WAL tail (partial record, bad crc) ends replay at the
last good record. Table files not named by the manifest are orphans
and are removed.

TestDBRecovery abandons the handle without Close and reopens;
TestDBTornWAL writes a WAL then truncates it mid-record.

## Manifest

The manifest is an append-only log of edits (added/deleted files,
watermarks). Every append updates an in-process clone of the current
version, so a read of Current() sees a consistent immutable snapshot
even mid-append. At 4 MiB the log rewrites as a snapshot. Versions
are immutable and refcounted through the reader cache: a compaction
that retires an input file marks the reader dead, and it closes when
the last iterator releases. Open iterators therefore stay valid
across the compactions that remove their files; what is not
supported is holding one across Close or recovery.

Concurrency invariants (locked by tests after real bugs):

- LogAndApply is serialized by a mutex; Current is an atomic
  pointer. The race detector passes with concurrent reads, writes
  and scans (TestDBConcurrent, TestManifestWatermarksThroughClone).
- Edit cloning preserves both watermarks. A dropped NextSeq once
  reset the sequence on every reopen and made flushed entries
  invisible; TestManifestWatermarksThroughClone refuses that
  regression.

## Compaction policies

Inputs are always "all or part of level i" plus "all or the overlap
of level i+1". runCompaction is policy-agnostic: it merges inputs
newest-wins (same user key: highest sequence wins, tombstones are
preserved, not dropped), rolls outputs at TargetFileSize and never
inside one user key's versions, publishes one manifest edit, then
deletes inputs. A crash mid-compaction loses only the output files;
inputs were never removed before the publish.

Version pruning is NOT done during compaction: every version of
every key survives, so snapshots never break. This costs space
(long-lived snapshots pin old versions in every level they reach)
and is the first thing to change if snapshot lifetimes become real.

## What is deliberately not here

Multi-process access, range deletion (tombstones are per-key),
compression (blocks are prefix-compressed keys only), whole-file
digests (integrity is crc32c framing per block, per WAL record and
per manifest record), and any tuner that changes geometry while
running. The planner computes geometry; it does not move it.
