// Package planner computes level geometry and bloom filter budgets
// for an LSM tree from workload size. The outputs are advisory: they
// feed Options at open time. Nothing in the engine depends on this
// package, so every recommendation here is checkable against a
// measured bench run of cmd/bench.
//
// The write cost model in this package prices one (1 + 1/r) rewrite
// per byte per transition. That describes a tree where every byte
// traverses every transition once: tiered and lazy full-level merges,
// or leveled when seeds span the whole keyspace. Moraine's leveled
// path merges one range-sliced seed at a time and rewrites only the
// overlap, so its measured WAF sits below this model; cmd/bench is
// the ground truth for leveled. The model is still the right shape
// for choosing ratios because all three policies share the same
// fanout requirement.
package planner

import "math"

// Geometry describes the workload and tree shape.
type Geometry struct {
	// Bytes is the steady-state live byte volume of user data.
	Bytes uint64
	// Memtable is the bytes a memtable holds before flushing; every
	// flush writes one run of about this size.
	Memtable uint64
	// Levels is the number of levels including L0. The first
	// transition is L0 into L1.
	Levels int
}

// MinTransitionRatio is the smallest size ratio worth considering.
// Below this a level merge rewrites nearly its own size for a tiny
// step down.
const MinTransitionRatio = 2.0

// EqualRatios returns the size ratio of every transition (slice
// index i is the ratio between level i+1 and level i, length
// Levels-1) under the classic cost model: one byte entering level i
// is rewritten once per level below it, a merge into level i+1 costs
// (1 + 1/r_i) bytes per input byte, and the product of ratios must
// carry Memtable up to Bytes. That cost sum is minimized, subject to
// the product constraint, by making every ratio equal; this is the
// AM-GM optimum and the closed form the DP in Ratios approximates.
func (g Geometry) EqualRatios() []float64 {
	out := make([]float64, g.Levels-1)
	fan := float64(g.Bytes) / float64(g.Memtable)
	if fan < MinTransitionRatio {
		for i := range out {
			out[i] = 1
		}
		return out
	}
	r := math.Pow(fan, 1/float64(g.Levels-1))
	for i := range out {
		out[i] = r
	}
	return out
}

// TargetBytes turns ratios into per-level byte targets; index 0 is
// the memtable size, index l is level l's target.
func (g Geometry) TargetBytes(ratios []float64) []uint64 {
	out := make([]uint64, len(ratios)+1)
	out[0] = g.Memtable
	for i, r := range ratios {
		out[i+1] = uint64(float64(out[i]) * r)
	}
	return out
}

// maxRatio bounds the discrete grid the DP searches.
const maxRatio = 64

// Ratios is the discrete counterpart of EqualRatios: it searches
// integer ratios on the 2..64 grid for the combination minimizing
// total WAF, 1 + sum_i (1 + 1/r_i), subject to prod r_i * Memtable
// >= Bytes with no overshoot: a ratio may not exceed what the
// remaining fanout still needs, so the product lands on capacity
// rather than far past it. Without that cap the WAF objective alone
// is monotone in every ratio and the answer degenerates to all-max.
// When Bytes needs more than maxRatio^(Levels-1) transitions of
// headroom the grid cannot cover it and Ratios returns the all-max
// fallback; the caller needs more levels.
func (g Geometry) Ratios() []float64 {
	fan := float64(g.Bytes) / float64(g.Memtable)
	n := g.Levels - 1
	if fan < MinTransitionRatio {
		return make([]float64, n) // tree never fills; nothing to size
	}
	if fan > math.Pow(maxRatio, float64(n)) {
		out := make([]float64, n)
		for i := range out {
			out[i] = maxRatio
		}
		return out
	}

	// State: the fanout still to cover after fixing transitions
	// 0..l, encoded as an integer. Scaling by 1e6 keeps fractional
	// products distinct; ceil collapses near-identical states.
	encode := func(remaining float64) int {
		if remaining < 1 {
			remaining = 1
		}
		return int(math.Ceil(remaining * 1e6))
	}

	type key [2]int
	memo := map[key]float64{}
	choice := map[key]float64{}

	var best func(l int, remaining float64) float64
	best = func(l int, remaining float64) float64 {
		if l == n {
			return 0 // construction guarantees coverage
		}
		k := key{l, encode(remaining)}
		if v, ok := memo[k]; ok {
			return v
		}
		// No overshoot: this transition may not consume more than
		// the remaining fanout needs, rounding up to the grid.
		hi := maxRatio
		if c := int(math.Ceil(remaining)); c < hi {
			hi = c
		}
		if hi < 2 {
			hi = 2
		}
		bestV := math.Inf(1)
		bestR := 0.0
		for r := 2; r <= hi; r++ {
			c := 1 + 1/float64(r) + best(l+1, remaining/float64(r))
			if c < bestV {
				bestV, bestR = c, float64(r)
			}
		}
		choice[k] = bestR
		memo[k] = bestV
		return bestV
	}

	best(0, fan)
	out := make([]float64, n)
	remaining := fan
	for l := 0; l < n; l++ {
		r := choice[key{l, encode(remaining)}]
		out[l] = r
		remaining /= r
	}
	return out
}

// WAF estimates leveled write amplification for a ratio set: the
// flush itself (1) plus one (1 + 1/r) rewrite per transition for
// every byte that reaches the bottom.
func WAF(ratios []float64) float64 {
	w := 1.0
	for _, r := range ratios {
		if r <= 0 {
			continue
		}
		w += 1 + 1/r
	}
	return w
}
