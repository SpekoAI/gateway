package upstream

import "testing"

func TestHTTPPolicyParse(t *testing.T) {
	t.Parallel()
	policy, err := NewHTTPPolicy("api.example.com", []string{"*.regional.example.com"}, false)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	accepted := []string{
		"https://api.example.com/v1/listen",
		"https://API.example.com/v1/listen",
		"https://api.example.com:443/v1/jobs",
		"https://eu1.regional.example.com/v2/jobs",
	}
	for _, raw := range accepted {
		if _, err := policy.Parse(raw); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
	}
	refused := []string{
		"http://api.example.com/v1/listen",
		"https://api.example.com:8443/v1/listen",
		"https://user:pw@api.example.com/v1/listen",
		"https://api.example.com/v1/listen?model=x",
		"https://api.example.com/v1/listen#frag",
		"https://evil.example.com/v1/listen",
		"https://regional.example.com/v2/jobs",
		"https://a.b.regional.example.com/v2/jobs",
		"wss://api.example.com/v1/listen",
		"/v1/listen",
	}
	for _, raw := range refused {
		if _, err := policy.Parse(raw); err == nil {
			t.Fatalf("%s: accepted, want refusal", raw)
		}
	}
}

func TestHTTPPolicyInsecureOverride(t *testing.T) {
	t.Parallel()
	policy, err := NewHTTPPolicy("api.example.com", []string{"127.0.0.1"}, true)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	if _, err := policy.Parse("http://127.0.0.1:39281/v1/listen"); err != nil {
		t.Fatalf("insecure override: %v", err)
	}
}

func TestHTTPPolicyRefusesMalformedHosts(t *testing.T) {
	t.Parallel()
	for _, host := range []string{"", "api.example.com/path", "*.", "*.a*b.com"} {
		if _, err := NewHTTPPolicy(host, nil, false); err == nil {
			t.Fatalf("%q: accepted, want refusal", host)
		}
	}
}
