package protocol

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// CapacityAttribution is public accounting metadata authenticated by the plan.
// It is not a grant, account credential, or proof that an old runtime enforces
// new fields. Only protocol-3 instance reports carry the resulting occupancy.
type CapacityAttribution struct {
	Account    string `json:"account"`
	Capability string `json:"capability"`
	Model      string `json:"model"`
	Slots      int64  `json:"slots"`
}

var capacityAccount = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$`)

func (c CapacityAttribution) Validate() error {
	if !capacityAccount.MatchString(c.Account) || (c.Capability != "stt_streams" && c.Capability != "tts_streams" && c.Capability != "realtime_sessions") || c.Model == "" || len(c.Model) > 256 || strings.TrimSpace(c.Model) != c.Model || !utf8.ValidString(c.Model) || strings.ContainsFunc(c.Model, unicode.IsControl) || c.Slots <= 0 {
		return fmt.Errorf("invalid capacity attribution")
	}
	return nil
}

// CapacityOccupancy carries actual verified active/opening sessions. Omitted
// attribution stays explicitly unknown; BYOK never becomes Speko account use.
type CapacityOccupancy struct {
	Account    string `json:"account"`
	Capability string `json:"capability"`
	Model      string `json:"model"`
	Active     int64  `json:"active"`
	Pending    int64  `json:"pending"`
}
type CapacitySnapshot struct {
	Complete       bool                `json:"complete"`
	Accounts       []CapacityOccupancy `json:"accounts"`
	BYOKActive     int64               `json:"byok_active"`
	BYOKPending    int64               `json:"byok_pending"`
	UnknownActive  int64               `json:"unknown_active"`
	UnknownPending int64               `json:"unknown_pending"`
}

func (c CapacitySnapshot) Validate(active int64, pending int) error {
	if len(c.Accounts) > 64 || c.Accounts == nil || c.BYOKActive < 0 || c.BYOKPending < 0 || c.UnknownActive < 0 || c.UnknownPending < 0 || c.Complete != (c.UnknownActive == 0 && c.UnknownPending == 0) {
		return fmt.Errorf("invalid capacity snapshot")
	}
	a, p := c.BYOKActive, c.BYOKPending
	add := func(total *int64, n int64) bool {
		if n < 0 || *total > math.MaxInt64-n {
			return false
		}
		*total += n
		return true
	}
	if !add(&a, c.UnknownActive) || !add(&p, c.UnknownPending) {
		return fmt.Errorf("capacity count overflow")
	}
	seen := map[string]bool{}
	for _, row := range c.Accounts {
		key := row.Account + "\x00" + row.Capability + "\x00" + row.Model
		if (CapacityAttribution{Account: row.Account, Capability: row.Capability, Model: row.Model, Slots: 1}).Validate() != nil || row.Active < 0 || row.Pending < 0 || row.Active == 0 && row.Pending == 0 || seen[key] || !add(&a, row.Active) || !add(&p, row.Pending) {
			return fmt.Errorf("invalid capacity occupancy")
		}
		seen[key] = true
	}
	// Counts describe sessions, while plan slot weights remain in reservations.
	if a != active || p != int64(pending) {
		return fmt.Errorf("capacity counts do not cover process state")
	}
	return nil
}
