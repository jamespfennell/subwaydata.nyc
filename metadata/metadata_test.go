package metadata

import (
	_ "embed"
	"encoding/json"
	"testing"
)

//go:embed nycsubway.json
var sampleConfig string

func TestSerializeRoundTrip(t *testing.T) {
	var c Metadata
	if err := json.Unmarshal([]byte(sampleConfig), &c); err != nil {
		t.Fatalf("failed to parse metadata: %s", err)
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		t.Fatalf("failed to write metadata: %s", err)
	}
	if string(b) != sampleConfig {
		t.Errorf("re-written metadata doesn't match original. Original\n%s\nRe-Written:\n%s", sampleConfig, string(b))
	}
}
