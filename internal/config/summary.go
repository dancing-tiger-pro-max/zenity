package config

import (
	"fmt"
	"strings"
)

// Summary renders the effective config for humans (used by --check-config).
func (c Config) Summary() string {
	var b strings.Builder
	p := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	p("config OK")
	p("  jetstream  %s (cursor every %s, max %d in flight, publish timeout %s)", c.Jetstream.URL, c.Jetstream.CursorInterval, c.Jetstream.MaxPending, c.Jetstream.PublishTimeout)
	p("             reconnect backoff %v, gap threshold %s", c.Jetstream.ReconnectBackoff, c.Jetstream.GapThreshold)
	p("  nats       %s", c.NATS.URL)
	p("  streams    dedupe window %s", c.Streams.DuplicatesWindow)
	for _, s := range []struct {
		name string
		l    StreamLimits
	}{{"POSTS", c.Streams.Posts}, {"LIKES", c.Streams.Likes}, {"DELETES", c.Streams.Deletes}, {"DLQ", c.Streams.DLQ}} {
		p("    %-8s max_age %-6s max_bytes %s", s.name, s.l.MaxAge, s.l.MaxBytes)
	}
	w := c.Workers
	p("  workers    ack_wait %s, max_deliver %d, backoff %v", w.AckWait, w.MaxDeliver, w.Backoff)
	p("    posts    keywords %v, languages %v, delay %s", w.Posts.Keywords, w.Posts.Languages, w.Posts.ProcessDelay)
	p("    likes    threshold %d per %s, delay %s", w.Likes.Threshold, w.Likes.Window, w.Likes.ProcessDelay)
	p("    deletes  tombstone_ttl %s, delay %s", w.Deletes.TombstoneTTL, w.Deletes.ProcessDelay)
	p("  monitor    every %s, near-full at %.0f%%, warn at %.0f%% unprocessed", c.Monitor.Interval, c.Monitor.FullRatio*100, c.Monitor.BacklogRatio*100)
	return b.String()
}
