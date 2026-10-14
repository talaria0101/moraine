package planner

import "math"

// BloomBudget describes a steady-state tree for filter sizing.
type BloomBudget struct {
	// Entries is the expected live entry count per level, index 0
	// is L0. Copies of one user key across levels each count: the
	// filter sits per file, not per user key.
	Entries []uint64
	// BitsPerEntry is the mean bits per entry the tree may spend on
	// filters. This is the Options.BitsPerKey budget, re-cut per
	// level instead of applied uniformly.
	BitsPerEntry float64
	// MinBitsPerEntry and MaxBitsPerEntry bound each level's share.
	// Zero fields take the defaults 1 and 32. The floor wins over
	// the budget when the budget cannot fund it.
	MinBitsPerEntry float64
	MaxBitsPerEntry float64
}

// fprConst is ln(0.6185), the per-bit false positive decay of a
// bloom filter with optimal hash count k = b*ln2.
const fprConst = -0.4805

// fpr is the false positive rate of an optimally configured bloom
// filter at b bits per entry.
func fpr(b float64) float64 {
	return math.Exp(fprConst * b)
}

// hashes is the optimal hash count at b bits per entry.
func hashes(b float64) int {
	k := int(math.Round(b * math.Ln2))
	if k < 1 {
		k = 1
	}
	return k
}

// visitProbs returns the probability a miss lookup probes each level:
// it always probes L0 and reaches level i+1 only if every filter
// above it passed.
func visitProbs(bits []float64) []float64 {
	p := make([]float64, len(bits))
	above := 1.0
	for i := range bits {
		p[i] = above
		above *= fpr(bits[i])
	}
	return p
}

// MissCost is the expected hash probes per absent-key lookup at this
// allocation: sum of visit probability times hash count per level.
// Both terms matter: extra bits tighten a filter (fewer visits below)
// but also add hash work paid on every visit. The optimum balances
// the two and its shape across levels is an output of that balance,
// not an assumption.
func MissCost(bits []float64) float64 {
	p := visitProbs(bits)
	c := 0.0
	for i := range bits {
		c += p[i] * float64(hashes(bits[i]))
	}
	return c
}

// budgetStep is the granularity of one allocation move, in bits.
const budgetStep = 0.25

// AllocateBits distributes BitsPerEntry across levels to minimize
// MissCost subject to the mean budget. It climbs greedily from the
// floor: repeatedly apply the single quarter-bit increase that cuts
// miss cost most while keeping the realized mean at or under budget,
// until no such move exists. Descent on the exact cost, so whatever
// shape falls out (including mid-level filters tighter than both
// ends) is what the cost model actually prefers.
func (bb BloomBudget) AllocateBits() []float64 {
	minB, maxB := bb.MinBitsPerEntry, bb.MaxBitsPerEntry
	if minB <= 0 {
		minB = 1
	}
	if maxB <= 0 {
		maxB = 32
	}
	n := len(bb.Entries)
	if n == 0 {
		return nil
	}

	var entries float64
	for _, e := range bb.Entries {
		entries += float64(e)
	}
	budgetBits := bb.BitsPerEntry * entries

	bits := make([]float64, n)
	for i := range bits {
		bits[i] = minB
	}

	realized := func(b []float64) float64 {
		var s float64
		for i := range b {
			s += float64(bb.Entries[i]) * b[i]
		}
		return s
	}

	for {
		cur := MissCost(bits)
		bestCost, bestI := cur, -1
		for i := range bits {
			if bits[i]+budgetStep > maxB {
				continue
			}
			cand := append([]float64(nil), bits...)
			cand[i] += budgetStep
			if realized(cand) > budgetBits {
				continue
			}
			if c := MissCost(cand); c < bestCost {
				bestCost, bestI = c, i
			}
		}
		if bestI < 0 {
			return bits
		}
		bits[bestI] += budgetStep
	}
}

// RealizedBitsPerEntry is the actual mean bits per entry of an
// allocation, for asserting the budget was met.
func RealizedBitsPerEntry(entries []uint64, bits []float64) float64 {
	var e, b float64
	for i := range entries {
		e += float64(entries[i])
		b += float64(entries[i]) * bits[i]
	}
	if e == 0 {
		return 0
	}
	return b / e
}
