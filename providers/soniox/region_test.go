package soniox

import "testing"

func TestRegionalHost(t *testing.T) {
	for _, testCase := range []struct {
		host, region, want string
		ok                 bool
	}{
		{"stt-rt.soniox.com", "jp", "stt-rt.jp.soniox.com", true},
		{"tts-rt.soniox.com", " EU ", "tts-rt.eu.soniox.com", true},
		{"api.soniox.com", "in", "api.in.soniox.com", true},
		{"stt-rt.soniox.com", "us", "", false},
		{"stt-rt.soniox.com", "", "", false},
		{"stt-rt.jp.soniox.com", "jp", "", false},
		{"soniox.com", "jp", "", false},
		{"stt-rt.soniox.com.evil.example", "jp", "", false},
	} {
		got, ok := RegionalHost(testCase.host, testCase.region)
		if got != testCase.want || ok != testCase.ok {
			t.Errorf("RegionalHost(%q, %q) = %q, %t; want %q, %t", testCase.host, testCase.region, got, ok, testCase.want, testCase.ok)
		}
	}
}

// A data-residency plan names the regional twin of the public host; every
// adapter has to accept it without operator configuration, and nothing else
// under soniox.com.
func TestAdaptersAcceptRegionalTwins(t *testing.T) {
	stt, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatal(err)
	}
	tts, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := NewBatch(BatchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	parsers := map[string]func(string) error{
		"stt":   func(raw string) error { _, err := stt.endpointPolicy.Parse(raw); return err },
		"tts":   func(raw string) error { _, err := tts.endpointPolicy.Parse(raw); return err },
		"batch": func(raw string) error { _, err := batch.endpointPolicy.Parse(raw); return err },
	}
	for name, accepted := range map[string][]string{
		"stt":   {"wss://stt-rt.soniox.com/transcribe-websocket", "wss://stt-rt.jp.soniox.com/transcribe-websocket", "wss://stt-rt.eu.soniox.com/transcribe-websocket"},
		"tts":   {"wss://tts-rt.soniox.com/tts-websocket", "wss://tts-rt.jp.soniox.com/tts-websocket"},
		"batch": {"https://api.soniox.com/v1/transcriptions", "https://api.jp.soniox.com/v1/transcriptions"},
	} {
		for _, raw := range accepted {
			if err := parsers[name](raw); err != nil {
				t.Errorf("%s refused %s: %v", name, raw, err)
			}
		}
	}
	for name, refused := range map[string]string{
		"stt":   "wss://stt-rt.xx.soniox.com/transcribe-websocket",
		"tts":   "wss://stt-rt.jp.soniox.com/tts-websocket",
		"batch": "https://api.jp.soniox.com.evil.example/v1/transcriptions",
	} {
		if err := parsers[name](refused); err == nil {
			t.Errorf("%s accepted %s", name, refused)
		}
	}
}
