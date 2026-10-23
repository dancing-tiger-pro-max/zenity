package ingest

import (
	"sync"
	"sync/atomic"
	"time"
)

// stats are counters for the periodic stats line. They're written from the
// read loop and the acker, so they're atomic.
type stats struct {
	mu        sync.Mutex
	bySubject map[string]int
	since     time.Time

	unrouted, parseErrors atomic.Int64
	oversized             atomic.Int64
	stored, duplicates    atomic.Int64
	lastSeenUS            atomic.Int64
}

func (s *stats) routed(subject string) {
	s.mu.Lock()
	s.bySubject[subject]++
	s.mu.Unlock()
}

func (s *stats) reset() {
	s.mu.Lock()
	s.bySubject = map[string]int{}
	s.since = time.Now()
	s.mu.Unlock()
	s.unrouted.Store(0)
	s.parseErrors.Store(0)
	s.oversized.Store(0)
	s.stored.Store(0)
	s.duplicates.Store(0)
}

// logStats writes one line for the interval and resets the counters.
func (in *Ingester) logStats() {
	s := &in.stats
	s.mu.Lock()
	attrs := []any{}
	total := s.unrouted.Load() + s.parseErrors.Load()
	for subject, n := range s.bySubject {
		total += int64(n)
		attrs = append(attrs, subject, n)
	}
	secs := time.Since(s.since).Seconds()
	s.mu.Unlock()

	attrs = append(attrs,
		"unrouted", s.unrouted.Load(),
		"parse_errors", s.parseErrors.Load(),
		"events_per_sec", int(float64(total)/secs),
		"ingest_lag_ms", msSince(s.lastSeenUS.Load()),
	)
	if !in.DryRun {
		attrs = append(attrs,
			"stored", s.stored.Load(),
			"duplicates", s.duplicates.Load(),
			"oversized", s.oversized.Load(),
			"confirmed_lag_ms", msSince(in.ConfirmedUS()),
		)
	}
	in.Log.Info("ingest stats", attrs...)
	s.reset()
}

// msSince converts a time_us into milliseconds behind now.
func msSince(timeUS int64) int64 {
	if timeUS == 0 {
		return -1
	}
	return time.Since(time.UnixMicro(timeUS)).Milliseconds()
}
