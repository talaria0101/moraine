package planner

import (
	"math"
	"testing"
)

// TestRatiosMatchClosedForm checks the DP against the AM-GM closed
// form on a regular geometry. Grid resolution means each DP ratio
// sits within one grid step of the continuous optimum, and the DP
// cost must not exceed the cost of the closed-form ratios rounded
// outward to the grid.
func TestRatiosMatchClosedForm(t *testing.T) {
	g := Geometry{Bytes: 1 << 30, Memtable: 1 << 20, Levels: 5}
	dp := g.Ratios()
	cf := g.EqualRatios()
	if len(dp) != len(cf) {
		t.Fatalf("dp returned %d ratios, want %d", len(dp), len(cf))
	}
	for i, r := range dp {
		// The no-overshoot grid rule shifts ratios off the continuous
		// optimum by up to two grid steps (e.g. 7,7,7,4 vs 5.66^4);
		// the exact optimum under the same rule is TestRatiosAgainstBruteForce.
		if math.Abs(r-cf[i]) > 2.0 {
			t.Errorf("ratio %d: dp %.3f vs closed form %.3f", i, r, cf[i])
		}
	}
	prod := 1.0
	for _, r := range dp {
		prod *= r
	}
	if prod*float64(g.Memtable) < float64(g.Bytes) {
		t.Errorf("dp product %.0f * memtable does not cover bytes", prod)
	}
	if wDP, wCF := WAF(dp), WAF(cf); wDP > wCF+1e-9 {
		t.Errorf("dp WAF %.4f exceeds closed-form WAF %.4f", wDP, wCF)
	}
}

// TestRatiosAgainstBruteForce enumerates every ratio combination
// under the same no-overshoot rule the DP enforces (a ratio may not
// exceed the remaining fanout) and holds the DP to the exact
// optimum. If the DP state encoding ever collapses distinct states
// this fails.
func TestRatiosAgainstBruteForce(t *testing.T) {
	g := Geometry{Bytes: 500, Memtable: 1, Levels: 4} // fanout 500, 3 transitions
	const lo, hi = 2, 12
	bestCost := math.Inf(1)
	var bestSet []float64
	var walk func(remaining float64, depth int, chosen []float64)
	walk = func(remaining float64, depth int, chosen []float64) {
		if depth == 3 {
			w := WAF(chosen)
			if w < bestCost {
				bestCost = w
				bestSet = append([]float64(nil), chosen...)
			}
			return
		}
		hi := hi
		if c := int(math.Ceil(remaining)); c < hi {
			hi = c
		}
		for r := lo; r <= hi; r++ {
			walk(remaining/float64(r), depth+1, append(chosen, float64(r)))
		}
	}
	walk(500, 0, nil)

	dp := g.Ratios()
	if w := WAF(dp); math.Abs(w-bestCost) > 1e-9 {
		t.Errorf("dp ratios %v cost %.6f, brute-force optimum %.6f at %v",
			dp, w, bestCost, bestSet)
	}
}

// TestBloomBeatsUniform checks the allocator against a uniform split
// at the same mean budget: on a realistic skew (deep level holds
// most entries) the reallocation must not cost more probes.
func TestBloomBeatsUniform(t *testing.T) {
	bb := BloomBudget{
		Entries:      []uint64{4 << 10, 64 << 10, 1 << 20},
		BitsPerEntry: 10,
	}
	got := bb.AllocateBits()
	if r := RealizedBitsPerEntry(bb.Entries, got); r > bb.BitsPerEntry+1e-6 {
		t.Fatalf("allocation exceeds budget: %v -> %.3f", got, r)
	}
	uniform := []float64{10, 10, 10}
	if cGot, cUni := MissCost(got), MissCost(uniform); cGot > cUni+1e-9 {
		t.Errorf("allocated miss cost %.3f exceeds uniform %.3f at %v", cGot, cUni, got)
	}
	t.Logf("allocation %v cost %.3f (uniform %.3f)", got, MissCost(got), MissCost(uniform))
}

// TestBloomAgainstBruteForce discretizes the allocation to 0.5-bit
// steps on a two-level tree and compares the allocator's cost to the
// exhaustive minimum under the same budget and floors.
func TestBloomAgainstBruteForce(t *testing.T) {
	entries := []uint64{1000, 10000}
	const budget = 8.0
	bb := BloomBudget{Entries: entries, BitsPerEntry: budget}
	got := bb.AllocateBits()

	best := math.Inf(1)
	for a := 1.0; a <= 32.0; a += 0.5 {
		b := (budget*1300 - 1000*a) / 10000
		if b < 1 { // the same floor the allocator starts from
			b = 1
		}
		if b > 32 {
			continue
		}
		if c := MissCost([]float64{a, b}); c < best {
			best = c
		}
	}
	if c := MissCost(got); c > best+1e-9 {
		t.Errorf("allocator cost %.4f exceeds brute-force grid min %.4f (got %v)", c, best, got)
	}
	t.Logf("allocator %v cost %.3f vs grid min %.3f", got, MissCost(got), best)
}

// TestBloomBudgetHonored checks clamping: with the default floor of
// one bit a sub-floor budget leaves every level at the floor (the
// floor wins over the budget), and the ceiling caps runaway levels.
func TestBloomBudgetHonored(t *testing.T) {
	entries := []uint64{1 << 10, 1 << 16, 1 << 20}
	low := BloomBudget{Entries: entries, BitsPerEntry: 0.5}
	got := low.AllocateBits()
	if r := RealizedBitsPerEntry(entries, got); r > 1+1e-6 {
		t.Errorf("sub-floor budget produced %.3f bits mean, floor is 1: %v", r, got)
	}
	high := BloomBudget{Entries: entries, BitsPerEntry: 64}
	for i, b := range high.AllocateBits() {
		if b > 32+1e-9 {
			t.Errorf("level %d allocated %.1f bits, cap is 32", i, b)
		}
	}
}
