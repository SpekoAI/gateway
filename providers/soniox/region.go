package soniox

import (
	"slices"
	"strings"
)

// Regions are Soniox's data-residency regions. Each serves a regional twin of
// every public host (stt-rt.jp.soniox.com for stt-rt.soniox.com) and answers
// only to keys from a project created in that region, so the party that mints
// the key also picks the host. Adapters accept every twin and dial whichever
// one the signed plan names.
var Regions = []string{"eu", "jp", "in"}

// RegionalHost returns the twin of a public Soniox host in region, or false
// when region is not a Soniox region or host is not a public Soniox host.
func RegionalHost(host, region string) (string, bool) {
	region = strings.ToLower(strings.TrimSpace(region))
	if !slices.Contains(Regions, region) {
		return "", false
	}
	service, ok := strings.CutSuffix(strings.ToLower(strings.TrimSpace(host)), ".soniox.com")
	if !ok || service == "" || strings.Contains(service, ".") {
		return "", false
	}
	return service + "." + region + ".soniox.com", true
}

// endpointHosts is the endpoint allowlist of an adapter whose public host is
// official: every regional twin, then the operator's own additions.
func endpointHosts(official string, additional []string) []string {
	hosts := make([]string, 0, len(Regions)+len(additional))
	for _, region := range Regions {
		twin, _ := RegionalHost(official, region)
		hosts = append(hosts, twin)
	}
	return append(hosts, additional...)
}
