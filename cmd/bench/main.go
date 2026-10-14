// Command bench runs an identical workload against each compaction
// policy and prints write amplification, space amplification and
// read amplification per policy.
//
// Workload: N unique keys inserted, then half of them overwritten,
// then a tenth deleted, then a point-lookup phase over live keys and
// an equal-sized phase over absent keys. All numbers are measured at
// the end of the run with counters from Stats, not estimated.
//
// Conditions of every number printed here: single writer goroutine,
// Sync=false (WAL writes go to the OS page cache), fixed random seed,
// on-disk page cache warm, this machine, this commit.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/talaria0101/moraine/internal/db"
	"github.com/talaria0101/moraine/internal/planner"
)

func main() {
	keys := flag.Int("keys", 50000, "unique keys in the fill phase")
	valSize := flag.Int("value-size", 100, "value bytes per key")
	gets := flag.Int("gets", 10000, "point lookups per read phase")
	debug := flag.Bool("debug", false, "print per-level byte totals per policy")
	seq := flag.Bool("seq", false, "ascending fill and update passes (range-partitioned runs)")
	hot := flag.Bool("hot", false, "ascending fill, updates concentrated on the first tenth (hot range)")
	plan := flag.Bool("plan", false, "print planner geometry and bloom advice for this workload")
	flag.Parse()

	if *plan {
		printPlan(*keys, *valSize)
	}

	policies := []db.CompactionPolicy{db.Leveled, db.Tiered, db.LazyLeveled}
	dist := "random"
	if *hot {
		dist = "hot-range"
	} else if *seq {
		dist = "sequential"
	}
	fmt.Printf("keys=%d value=%dB gets=%d/phase sync=off fill=%s\n\n", *keys, *valSize, *gets, dist)
	fmt.Println("policy   waf  waf_comp  space_amp  probe/hit  probe/miss  hit_us  miss_us  flushes  compactions")

	for _, p := range policies {
		row, levels, err := runOne(p, *keys, *valSize, *gets, *seq, *hot)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", p, err)
			os.Exit(1)
		}
		fmt.Println(row)
		if *debug {
			fmt.Printf("  levels: %v\n", levels)
		}
	}
}

// benchMemtable and benchLevels must match the options runOne opens
// the database with; the plan is only meaningful for the run beside it.
const (
	benchMemtable = 128 << 10
	benchLevels   = 7
)

// printPlan prints planner recommendations for the bench workload:
// level targets from the ratio DP and a per-level bloom split of the
// 10 bits/entry default budget.
func printPlan(keys, valSize int) {
	klen := 10
	live := uint64(keys-keys/10) * uint64(klen+valSize)
	g := planner.Geometry{Bytes: live, Memtable: benchMemtable, Levels: benchLevels}
	ratios := g.Ratios()
	targets := g.TargetBytes(ratios)
	fmt.Printf("plan: live %.1f MiB, memtable %d KiB, %d levels\n",
		float64(live)/(1<<20), benchMemtable/(1<<10), benchLevels)
	fmt.Printf("plan: dp ratios %v -> predicted WAF %.2f\n", ratios, planner.WAF(ratios))
	fmt.Printf("plan: level targets (KiB):")
	for _, t := range targets {
		fmt.Printf(" %d", t/(1<<10))
	}
	fmt.Println()

	// Entries per level: targets carry user data only; estimate
	// entries as target bytes over the per-entry on-disk footprint.
	entryBytes := uint64(klen + valSize + 16) // key, value, trailer and record overhead
	estimates := make([]uint64, len(targets))
	for i, t := range targets {
		estimates[i] = t / entryBytes
	}
	bb := planner.BloomBudget{Entries: estimates, BitsPerEntry: 10}
	bits := bb.AllocateBits()
	fmt.Printf("plan: bloom bits/entry per level %v (mean budget 10)\n", bits)
	fmt.Println()
}

// runOne runs the full workload against one policy in a fresh
// directory and returns one report line plus the per-level byte
// totals at measurement time. seq switches the fill and update
// passes from random key order to ascending order, which makes every
// flushed run cover a disjoint key range. hot fills ascending but
// pushes every update into the first tenth of the keyspace.
func runOne(policy db.CompactionPolicy, keys, valSize, gets int, seq, hot bool) (string, []uint64, error) {
	dir, err := os.MkdirTemp("", "moraine-bench-")
	if err != nil {
		return "", nil, err
	}
	defer os.RemoveAll(dir)

	opts := db.Options{Compaction: policy}
	// Small level targets so the ~6 MB working set actually flows
	// through several levels; with the shipped defaults (8 MiB L1)
	// nothing ever compacts and every policy scores identically.
	opts.WriteBuffer = 128 << 10
	opts.TargetFileSize = 64 << 10
	opts.L1TargetBytes = 256 << 10
	opts.LevelMultiplier = 4
	opts.L0CompactionTrigger = 4
	d, err := db.Open(dir, opts)
	if err != nil {
		return "", nil, err
	}

	rng := rand.New(rand.NewSource(42))
	klen := 10 // "keyNNNNNN" is within this
	val := make([]byte, valSize)

	put := func(k string) error {
		rng.Read(val)
		return d.Put([]byte(k), val)
	}

	// Phase 1: fill with unique keys, random or ascending order.
	for i := 0; i < keys; i++ {
		n := i
		if !seq && !hot {
			n = rng.Intn(keys)
		}
		if err := put(fmt.Sprintf("key%06d", n)); err != nil {
			return "", nil, err
		}
	}
	// Phase 2: overwrite a random half, matching the fill order;
	// hot mode instead hammers the first tenth of the keyspace.
	updates := keys / 2
	if hot {
		updates = keys / 5
	}
	for i := 0; i < updates; i++ {
		var k string
		switch {
		case hot:
			k = fmt.Sprintf("key%06d", i%(keys/10))
		case seq:
			k = fmt.Sprintf("key%06d", i)
		default:
			k = fmt.Sprintf("key%06d", rng.Intn(keys))
		}
		if err := put(k); err != nil {
			return "", nil, err
		}
	}
	// Phase 3: delete a random tenth.
	deleted := map[int]bool{}
	for len(deleted) < keys/10 {
		deleted[rng.Intn(keys)] = true
	}
	for i := range deleted {
		if err := d.Delete([]byte(fmt.Sprintf("key%06d", i))); err != nil {
			return "", nil, err
		}
	}

	// Let the background tree settle, then measure. The loop waits
	// for quiescence and confirms the compaction counter stops
	// moving, closing the pick race that WaitIdle documents.
	for i := 0; i < 100; i++ {
		d.WaitIdle()
		c := d.Stats().Compactions
		time.Sleep(20 * time.Millisecond)
		d.WaitIdle()
		if d.Stats().Compactions == c {
			break
		}
	}
	writeStats := d.Stats()
	levels := d.LevelSizes()

	var onDisk uint64
	for _, l := range levels {
		onDisk += l
	}
	logicalWrites := (keys + updates + len(deleted)) * (klen + valSize)
	liveKeys := keys - len(deleted)
	liveBytes := liveKeys * (klen + valSize)
	waf := float64(writeStats.WALBytes+writeStats.SSTBytesWritten) / float64(logicalWrites)
	wafComp := float64(writeStats.CompactionBytes) / float64(logicalWrites)
	spaceAmp := float64(onDisk) / float64(liveBytes)

	// Read phase A: hits over live keys. Phase B: misses.
	hits := make([]string, 0, gets)
	for len(hits) < gets {
		i := rng.Intn(keys)
		if !deleted[i] {
			hits = append(hits, fmt.Sprintf("key%06d", i))
		}
	}
	hitAmp, hitUs, err := readPhase(d, hits)
	if err != nil {
		return "", nil, err
	}
	misses := make([]string, gets)
	for i := range misses {
		misses[i] = fmt.Sprintf("zzz%06d", rng.Intn(keys))
	}
	missAmp, missUs, err := readPhase(d, misses)
	if err != nil {
		return "", nil, err
	}

	end := d.Stats()
	if err := d.Close(); err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("%-8s %5.2f  %8.2f  %8.2f  %9.2f  %10.2f  %6.1f  %7.1f  %7d  %11d",
		policy, waf, wafComp, spaceAmp, hitAmp, missAmp, hitUs, missUs,
		end.Flushes, end.Compactions), levels, nil
}

// readPhase runs one sequence of point lookups and returns read
// amplification (probes per get) and microseconds per get, measured
// from counter deltas around the phase.
func readPhase(d *db.DB, keys []string) (amp float64, usPerGet float64, err error) {
	before := d.Stats()
	start := time.Now()
	for _, k := range keys {
		if _, _, err := d.Get([]byte(k)); err != nil {
			return 0, 0, err
		}
	}
	elapsed := time.Since(start)
	after := d.Stats()
	n := float64(len(keys))
	amp = float64(after.SourcesProbed-before.SourcesProbed) / n
	usPerGet = float64(elapsed.Microseconds()) / n
	return amp, usPerGet, nil
}
