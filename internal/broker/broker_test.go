package broker

import (
	"testing"

	"bskyrouter/internal/config"
	"bskyrouter/internal/router"
)

// Every subject the router publishes to must be captured by exactly one stream.
// A route without a stream would publish into nothing; a subject in two streams
// is rejected by NATS.
func TestEveryRouteHasExactlyOneStream(t *testing.T) {
	streams := streamConfigs(config.Config{})
	for _, r := range router.Table {
		var owners []string
		for _, s := range streams {
			for _, subj := range s.Subjects {
				if subj == r.Subject {
					owners = append(owners, s.Name)
				}
			}
		}
		if len(owners) != 1 {
			t.Errorf("subject %q is captured by %v, want exactly one stream", r.Subject, owners)
		}
	}
}
