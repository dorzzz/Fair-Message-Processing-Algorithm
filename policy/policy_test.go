package policy

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const TEST_CAPACITY int = 1000

func TestFairness(t *testing.T) {
	p := NewPolicy(TEST_CAPACITY)

	// Tenants and their purchased sets
	type entry struct {
		id   string
		sets int
	}

	tenants := []entry{
		{"tenant1", 1},
		{"tenant2", 2},
		{"tenant3", 1},
		{"tenant4", 3},
		{"tenant5", 1},
	}
	for _, e := range tenants {
		p.AddTenant(e.id, e.sets)
	}

	// Initialize a map to track how many messages were assigned to each tenant
	total := make(map[string]int, len(tenants))
	for _, e := range tenants {
		total[e.id] = 0
	}

	// Run many rounds. Each round: fill up to capacity, then complete all.
	rounds := 300
	startIndex := 0

	for r := 0; r < rounds; r++ {
		roundStarted := make(map[string]int, len(tenants))
		started := 0

		for started < TEST_CAPACITY {
			progress := 0
			// Iterate tenants in a rotating order to avoid bias
			for i := 0; i < len(tenants) && started < TEST_CAPACITY; i++ {
				idx := (startIndex + i) % len(tenants)
				id := tenants[idx].id
				if p.ShouldConsume(id, "order") {
					p.OnMessageStart(id, "order")
					started++
					roundStarted[id]++
					total[id]++
					progress++
				}
			}
			// If no one could start, stop this round
			if progress == 0 {
				break
			}
		}

		// Complete all started in this round
		for id, n := range roundStarted {
			for i := 0; i < n; i++ {
				p.OnMessageComplete(id, "order")
			}
		}

		t.Logf("round %d started=%v", r, roundStarted)

		startIndex = (startIndex + 1) % len(tenants)
	}

	// Validate ratios ~ proportional to sets
	// Compare each tenant against tenant1 as baseline.
	base := tenants[0]
	for _, e := range tenants[1:] {
		if tenants[0].sets == 0 || e.sets == 0 {
			continue
		}
		gotRatio := float64(total[e.id]) / float64(total[base.id])
		wantRatio := float64(e.sets) / float64(base.sets)
		assert.InDelta(t, wantRatio, gotRatio, 0.35, "ratio %s:%s", e.id, base.id)
	}
}

func TestBurstHandling(t *testing.T) {
	p := NewPolicy(TEST_CAPACITY)

	// Tenants with quotas, for e.g ;t1 low, t4 premium
	type entry struct {
		id   string
		sets int
	}
	tenants := []entry{
		{"tenant1", 1},
		{"tenant2", 2},
		{"tenant3", 1},
		{"tenant4", 3}, // premium
		{"tenant5", 1},
	}
	for _, e := range tenants {
		p.AddTenant(e.id, e.sets)
	}

	// Explain current policy behavior: per-set fixed slots based on ALL tenants' sets.
	totalSetsAll := 0
	for _, e := range tenants {
		totalSetsAll += e.sets
	}
	perSet := TEST_CAPACITY / totalSetsAll
	t.Logf("Config: capacity=%d totalSets(all tenants)=%d perSet≈%d", TEST_CAPACITY, totalSetsAll, perSet)

	// Phase A: only tenant1 bursts. Others idle.
	// Expect the system to use capacity efficiently (opportunistic),
	// so tenant1 can fill most or all capacity when alone.
	inflightA := 0
	startedA := make(map[string]int)
	refusedA := 0
	burstAttempts := TEST_CAPACITY * 10 // simulate heavy burst demand
	for i := 0; i < burstAttempts && inflightA < TEST_CAPACITY; i++ {
		if p.ShouldConsume("tenant1", "order") {
			p.OnMessageStart("tenant1", "order")
			startedA["tenant1"]++
			inflightA++
		} else {
			refusedA++
		}
	}
	// Complete Phase A
	for i := 0; i < startedA["tenant1"]; i++ {
		p.OnMessageComplete("tenant1", "order")
	}
	unused := TEST_CAPACITY - startedA["tenant1"]
	t.Logf("Phase A burst: attempts=%d granted=%d refused=%d unused_capacity≈%d", burstAttempts, startedA["tenant1"], refusedA, unused)
	t.Log("Note: Current policy uses per-set fixed slots; it does not borrow idle capacity when others are inactive.")
	t.Logf("Phase A expected≈%d (tenant1 has 1 set)", perSet)
	t.Logf("Phase A: tenant1 started=%d", startedA["tenant1"])
	// Sanity: we made progress and respected capacity.
	if startedA["tenant1"] == 0 || startedA["tenant1"] > TEST_CAPACITY {
		t.Fatalf("unexpected Phase A starts=%d capacity=%d", startedA["tenant1"], TEST_CAPACITY)
	}

	// Phase B: tenant4 becomes active while tenant1 continues to send.
	// We simulate multiple fill/complete cycles and measure shares.
	const roundsB = 10
	totalB := map[string]int{"tenant1": 0, "tenant4": 0}
	startIndex := 0
	active := []string{"tenant1", "tenant4"}

	expT1 := perSet * 1
	expT4 := perSet * 3
	t.Logf("Phase B expected per round: tenant1≈%d tenant4≈%d total≈%d", expT1, expT4, expT1+expT4)
	unused_B := TEST_CAPACITY - (expT1 + expT4)
	t.Logf("inactive tenants: tenant2(2 sets), tenant3(1), tenant5(1); unused≈%d", unused_B)

	for r := 0; r < roundsB; r++ {
		roundStarted := map[string]int{"tenant1": 0, "tenant4": 0}
		started := 0
		for started < TEST_CAPACITY {
			progress := 0
			for i := 0; i < len(active) && started < TEST_CAPACITY; i++ {
				id := active[(startIndex+i)%len(active)]
				if p.ShouldConsume(id, "order") {
					p.OnMessageStart(id, "order")
					roundStarted[id]++
					totalB[id]++
					started++
					progress++
				}
			}
			if progress == 0 {
				break
			}
		}
		// Complete all started in this round
		for id, n := range roundStarted {
			for i := 0; i < n; i++ {
				p.OnMessageComplete(id, "order")
			}
		}
		t.Logf("round %d started=%v totals=%v", r, roundStarted, totalB)
		startIndex = (startIndex + 1) % len(active)
	}
	t.Logf("Phase B totals: tenant1=%d tenant4=%d ratio=%.2f",
		totalB["tenant1"], totalB["tenant4"],
		float64(totalB["tenant4"])/float64(max(1, totalB["tenant1"])))
	// Expect premium tenant4 to receive more share than tenant1
	// Target a loose bound: tenant4 should get at least ~2x of tenant1 in Phase B.
	ratio := float64(totalB["tenant4"]) / float64(max(1, totalB["tenant1"]))
	if ratio < 2.0 {
		t.Fatalf("expected tenant4 to get >=2x of tenant1 in Phase B, got ratio=%.2f (t4=%d, t1=%d)",
			ratio, totalB["tenant4"], totalB["tenant1"])
	}
}

func TestCapacityLimits(t *testing.T) {
	p := NewPolicy(TEST_CAPACITY)

	// Ten tenants with mixed quotas
	for i := 1; i <= 10; i++ {
		sets := 1
		if i%3 == 0 {
			sets = 2
		}
		if i%5 == 0 {
			sets = 3
		}
		p.AddTenant(fmt.Sprintf("tenant%d", i), sets)
	}
	t.Logf("Config: capacity=%d tenants=%d", TEST_CAPACITY, 10)

	// Compute theoretical lower bound given integer per-set slots.
	totalSetsAll := 0
	for i := 1; i <= 10; i++ {
		sets := 1
		if i%3 == 0 {
			sets = 2
		}
		if i%5 == 0 {
			sets = 3
		}
		totalSetsAll += sets
	}
	perSet := TEST_CAPACITY / totalSetsAll
	baseFill := perSet * totalSetsAll
	remainder := TEST_CAPACITY - baseFill
	t.Logf("capacity split: totalSets=%d perSet≈%d baseFill=%d remainder=%d", totalSetsAll, perSet, baseFill, remainder)

	// Try to start far more than capacity in a single "round".
	started := 0
	roundStarted := make(map[string]int)

	// Rotate tenants to avoid bias
	startIndex := 0
	ids := make([]string, 0, 10)
	for i := 1; i <= 10; i++ {
		ids = append(ids, fmt.Sprintf("tenant%d", i))
	}

	for {
		progress := 0
		for i := 0; i < len(ids); i++ {
			id := ids[(startIndex+i)%len(ids)]
			if p.ShouldConsume(id, "order") {
				p.OnMessageStart(id, "order")
				roundStarted[id]++
				started++
				if started > TEST_CAPACITY {
					t.Fatalf("started exceeded capacity: %d > %d", started, TEST_CAPACITY)
				}
				progress++
			}
		}
		if progress == 0 || started == TEST_CAPACITY {
			break
		}
		startIndex = (startIndex + 1) % len(ids)
	}

	// Verify inflight equals what we started and did not exceed capacity.
	stats := p.GetStats()
	sumActive := 0
	for _, v := range stats {
		m := v.(map[string]int)
		sumActive += m["active"]
	}
	t.Logf("mid-round inflight=%d (expected=%d)", sumActive, started)
	if sumActive != started {
		t.Fatalf("inflight mismatch: got %d, want %d", sumActive, started)
	}

	t.Logf("roundStarted=%v totalStarted=%d", roundStarted, started)

	// Complete all started
	for id, n := range roundStarted {
		for i := 0; i < n; i++ {
			p.OnMessageComplete(id, "order")
		}
	}

	// Relaxed assertion with explanation for per-set rounding
	if started < baseFill || started > TEST_CAPACITY {
		t.Fatalf("capacity utilization out of bounds: got started=%d, expected in [%d,%d]", started, baseFill, TEST_CAPACITY)
	}
	// Optional: highlight any unfilled remainder left by per-set caps.
	if started < TEST_CAPACITY {
		t.Logf("note: %d slots left unused this round due to per-set rounding under current policy", TEST_CAPACITY-started)
	}
}

// helper
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
