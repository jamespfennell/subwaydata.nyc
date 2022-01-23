package config

import (
	_ "embed"
	"encoding/json"
	"testing"
)

//go:embed sample.json
var sampleConfig string

func TestConfig(t *testing.T) {
	var c Config
	if err := json.Unmarshal([]byte(sampleConfig), &c); err != nil {
		t.Fatalf("failed to parse config: %s", err)
	}
	b, err := json.MarshalIndent(c, "", "    ")
	if err != nil {
		t.Fatalf("failed to write config: %s", err)
	}
	if string(b) != sampleConfig {
		t.Errorf("re-written config doesn't match original. Original\n%s\nRe-Written:\n%s", sampleConfig, string(b))
	}
}
