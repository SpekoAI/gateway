package gateway

import (
	"github.com/SpekoAI/gateway/protocol"
	"sort"
)

type trackedCapacity struct {
	source      protocol.CredentialSource
	attribution *protocol.CapacityAttribution
}

func verifiedCapacity(c *protocol.CapacityAttribution, source protocol.CredentialSource) trackedCapacity {
	var copy *protocol.CapacityAttribution
	if c != nil {
		v := *c
		copy = &v
	}
	return trackedCapacity{source: source, attribution: copy}
}

// Caller holds s.mu, so active and pending describe one process instant.
func (s *Server) capacityLocked() protocol.CapacitySnapshot {
	result := protocol.CapacitySnapshot{Accounts: []protocol.CapacityOccupancy{}}
	rows := map[string]*protocol.CapacityOccupancy{}
	add := func(c trackedCapacity, pending bool) {
		if c.source == protocol.CredentialsBYOK {
			if pending {
				result.BYOKPending++
			} else {
				result.BYOKActive++
			}
			return
		}
		if c.attribution == nil {
			if pending {
				result.UnknownPending++
			} else {
				result.UnknownActive++
			}
			return
		}
		a := c.attribution
		key := a.Account + "\x00" + a.Capability + "\x00" + a.Model
		row := rows[key]
		if row == nil {
			row = &protocol.CapacityOccupancy{Account: a.Account, Capability: a.Capability, Model: a.Model}
			rows[key] = row
		}
		if pending {
			row.Pending++
		} else {
			row.Active++
		}
	}
	for _, session := range s.sessions {
		add(session.capacity, false)
	}
	for _, pending := range s.inflight {
		if pending.pending {
			add(pending.capacity, true)
		}
	}
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		row := rows[key]
		if i >= 64 {
			result.UnknownActive += row.Active
			result.UnknownPending += row.Pending
			continue
		}
		result.Accounts = append(result.Accounts, *row)
	}
	result.Complete = result.UnknownActive == 0 && result.UnknownPending == 0
	return result
}
