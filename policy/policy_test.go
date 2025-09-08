package policy

import (
	"testing"

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
