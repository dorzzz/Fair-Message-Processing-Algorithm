package policy

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
)

const TEST_CAPACITY int = 100

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

	// Tenants with quotas: t1 low, t4 premium
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

	// Phase A: only tenant1 bursts. Others idle.
	// Expect the system to use capacity efficiently (opportunistic),
	// so tenant1 can fill most or all capacity when alone.
	inflightA := 0
	startedA := make(map[string]int)
	for inflightA < TEST_CAPACITY {
		if p.ShouldConsume("tenant1", "order") {
			p.OnMessageStart("tenant1", "order")
			startedA["tenant1"]++
			inflightA++
		} else {
			break
		}
	}
	// Complete Phase A
	for i := 0; i < startedA["tenant1"]; i++ {
		p.OnMessageComplete("tenant1", "order")
	}
	// Sanity: we made progress and respected capacity.
	if startedA["tenant1"] == 0 || startedA["tenant1"] > TEST_CAPACITY {
		t.Fatalf("unexpected Phase A starts=%d capacity=%d", startedA["tenant1"], TEST_CAPACITY)
	}

	// Phase B: tenant4 becomes active while tenant1 continues to send.
	// We simulate multiple fill/complete cycles and measure shares.
	const roundsB = 60
	totalB := map[string]int{"tenant1": 0, "tenant4": 0}
	startIndex := 0
	active := []string{"tenant1", "tenant4"}

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
		startIndex = (startIndex + 1) % len(active)
	}
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

	// Complete all started
	for id, n := range roundStarted {
		for i := 0; i < n; i++ {
			p.OnMessageComplete(id, "order")
		}
	}

	// Final assertion: never exceeded global capacity and filled it reasonably.
	if started == 0 {
		t.Fatalf("no messages started, expected up to capacity=%d", TEST_CAPACITY)
	}
	if started > TEST_CAPACITY {
		t.Fatalf("started=%d exceeded capacity=%d", started, TEST_CAPACITY)
	}
}

// helper
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
