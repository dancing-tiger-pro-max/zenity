package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The repo's config.yaml must always load: it's what the README and the cluster use.
func TestRepoConfigLoads(t *testing.T) {
	c, err := Load("../../config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.Streams.Likes.MaxBytes != 2<<30 {
		t.Errorf("likes max_bytes = %v, want 2GB", c.Streams.Likes.MaxBytes)
	}
}

// Cross-field rules are the ones easy to break by editing one value.
func TestValidateCrossFieldRules(t *testing.T) {
	raw, err := os.ReadFile("../../config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, from, to, wantErr string
	}{
		{"tombstones expire before posts can lag", "tombstone_ttl: 24h", "tombstone_ttl: 1h", "tombstone_ttl"},
		{"delay longer than ack wait", "process_delay: 0s", "process_delay: 1m", "process_delay"},
		{"dedupe window shorter than cursor interval", "duplicates_window: 2m", "duplicates_window: 500ms", "duplicates_window"},
		{"unknown key", "threshold: 20", "treshold: 20", "treshold"},
		{"ack wait longer than likes window", "ack_wait: 30s", "ack_wait: 2m", "ack_wait"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			edited := strings.Replace(string(raw), tt.from, tt.to, 1)
			if edited == string(raw) {
				t.Fatalf("test setup: %q not found in config.yaml", tt.from)
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			os.WriteFile(path, []byte(edited), 0o644)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}
