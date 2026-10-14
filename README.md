# moraine

An LSM-tree key/value engine in Go, standard library only. It is a
from-scratch build of the storage-engine stack: WAL, skiplist
memtable, sstables with bloom filters, a manifest of live files, and
background flush and compaction. It is single-process: one DB per
directory, no concurrent Open.

## Status

Working engine with model-checked correctness: the DB-level test
drives every mutation through a reference model and compares point
reads, snapshot reads and full scans, under three compaction
policies, with the race detector on. Recovery is exercised by
abandoning the handle (WAL replay) and by a torn WAL tail.

## Layout

    internal/key       internal keys: user key + sequence + type trailer
    internal/bloom     bloom filters, cache-line blocked probes
    internal/block     prefix-compressed blocks with restart points
    internal/table     sstable writer/reader: blocks, filter, index
    internal/mem       skiplist memtable with snapshot-visible iteration
    internal/wal       crc'd length-prefixed log, torn-tail recovery
    internal/manifest  append-only edit log, snapshot rewrite, atomic versions
    internal/db        the engine: flush, compaction policies, scans, stats
    internal/planner   advisory level-geometry and bloom-budget math
    cmd/bench          write/space/read amplification harness per policy

## Usage

    db, err := db.Open("/path/to/dir", db.Options{Sync: true})
    ...
    err = db.Put([]byte("key"), []byte("value"))
    val, found, err := db.Get([]byte("key"))
    err = db.Delete([]byte("key"))
    it, err := db.NewIter()            // full-range merge scan
    for it.First(); it.Valid(); it.Next() {
            k, v := it.Key(), it.Value()
    }
    err = db.Close()

Options cover WriteBuffer, BlockSize, BitsPerKey, Sync (WAL fsync per
write), TargetFileSize, L0CompactionTrigger, MaxLevels, L1TargetBytes,
LevelMultiplier, Compaction (leveled, tiered, lazy-leveled) and
TieredFiles. Zero fields take documented defaults.

Stats() reports WAL bytes, sstable bytes written, flushes,
compactions, compaction input bytes, and Gets/SourcesProbed: read
amplification is SourcesProbed/Gets. LevelSizes() gives per-level
bytes on disk.

## Compaction policies

All three keep levels 1+ disjoint, so the reader's binary search
per level stays valid.

- Leveled (default): L0 merges as a unit; deeper levels merge one
  seed file plus its overlap in the next level. Lowest write
  amplification, most compactions.
- Tiered: a level accumulates TieredFiles files, then merges into
  the next level wholesale. Fewest compactions, worst read
  amplification (data piles up in deep levels as whole-range runs).
- LazyLeveled: fills like tiered but merges only the overlapping
  part of the next level. Between the other two; saves bytes over
  tiered only when writes are range-local (measured below).

The picker scores each level (leveled: file count at L0, bytes at
deeper levels; tiered and lazy: file count) and compacts the highest
score at or above 1.0, shallowest on ties.

## Measured amplification

cmd/bench, 50000 keys of ~110 bytes, a half overwritten, a tenth
deleted, 10000 point gets per phase, Sync off, compact geometry
(128 KiB memtable, 64 KiB target file, 256 KiB L1, multiplier 4):

Five runs, random fill. WAF is write bytes over put bytes, median
with observed min to max in parentheses. Probe columns are
Stats.SourcesProbed per get at quiescence; a compaction in flight
raises them (one run saw 4.99/4.00 mid-compact). waf_comp, space_amp
and the timing columns are single-run representatives and carry the
same timing spread as waf:

    policy   waf                waf_comp  space_amp  probe/hit  probe/miss  hit_us  miss_us
    leveled  7.19 (6.69-8.20)   5.3       1.70       2.00       1.00        5.5     0.1
    tiered   9.32 (8.56-9.47)   7.2       1.70       2.00       1.00        5.9     0.1
    lazy     9.09 (8.55-9.26)   7.0       1.70       2.00       1.00        5.0     0.1

The leveled median sits under both other medians and under their
minima; tiered and lazy overlap. WAF moves with background
compaction timing, so treat single-run differences under about 1.0
as noise. Findings that held across runs and key distributions
(random, sequential, hot-range):

- Leveled is consistently the lowest WAF, at roughly ten times the
  compaction count of the other two.
- Tiered and lazy sit within noise of each other on uniform keys:
  with full-level inputs the input range spans everything, so the
  overlap saving only materializes on hot-range writes, where lazy
  measured 8.41 vs tiered 8.62.
- Space amplification converges to ~1.7 for all policies at
  quiescence on this workload; the bulk is per-record and block
  encoding overhead, not stale copies. Policies differ in space only
  mid-flight.
- The planner's WAF model predicts 9.31 for the full-level-input
  discipline; tiered measured 9.26 under the same workload.

Numbers above are from one sandboxed linux/amd64 runner, go1.27.1,
page cache warm. Reproduce with `go run ./cmd/bench`.

## Design notes

docs/design.md walks the write path, read path, recovery, and the
invariants the tests lock. internal/planner is advisory: its two
optimizers (level ratios, bloom bits per level) are validated against
brute force on small instances in their own tests, and its cost
model is documented with the discipline it assumes.

## Building and testing

    go test -race ./...
    go vet ./...
    go run ./cmd/bench

Requires Go 1.27+. No dependencies outside the standard library.
