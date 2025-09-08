package policy

import (
	"sync"
)

type Tenant struct {
	ID             string
	ProcessingSets int
	active         int
}

type MessageConsumptionPolicy struct {
	mu        sync.Mutex
	capacity  int
	tenants   map[string]*Tenant
	totalSets int
}

func NewPolicy(capacity int) *MessageConsumptionPolicy {
	return &MessageConsumptionPolicy{
		capacity: capacity,
		tenants:  make(map[string]*Tenant),
	}
}

func (p *MessageConsumptionPolicy) AddTenant(id string, sets int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.tenants[id] = &Tenant{ID: id, ProcessingSets: sets}
	p.totalSets += sets
}

func (p *MessageConsumptionPolicy) ShouldConsume(tenantID, messageType string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
    
    // if tenant is not in hash-map do not allow consuming 
	t, ok := p.tenants[tenantID]
	if !ok {
		return false
	}

	allowed := (t.ProcessingSets * p.capacity) / p.totalSets
	return t.active < allowed
}

func (p *MessageConsumptionPolicy) OnMessageStart(tenantID, messageType string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if t, ok := p.tenants[tenantID]; ok {
		t.active++
	}
}

func (p *MessageConsumptionPolicy) OnMessageComplete(tenantID, messageType string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if t, ok := p.tenants[tenantID]; ok && t.active > 0 {
		t.active--
	}
}

func (p *MessageConsumptionPolicy) GetStats() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats := make(map[string]interface{})
	for id, t := range p.tenants {
		stats[id] = map[string]int{
			"active": t.active,
			"sets":   t.ProcessingSets,
		}
	}
	return stats
}
