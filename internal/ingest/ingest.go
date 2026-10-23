// Package ingest holds the single Jetstream connection, routes each event and
// hands it on. In dry-run mode it only counts where events would go.
package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/coder/websocket"

	"bskyrouter/internal/event"
	"bskyrouter/internal/router"
)

const statsEvery = 5 * time.Second

type Ingester struct {
	URL    string
	Routes []router.Route
	DryRun bool
	Log    *slog.Logger
}

// Run connects to Jetstream and processes events until ctx is cancelled or the
// connection fails.
func (in *Ingester) Run(ctx context.Context) error {
	u, err := subscribeURL(in.URL, router.WantedCollections(in.Routes))
	if err != nil {
		return err
	}
	in.Log.Info("connecting", "url", u, "dry_run", in.DryRun)
	conn, _, err := websocket.Dial(ctx, u, nil)
	if err != nil {
		return fmt.Errorf("dial jetstream: %w", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 22) // a few posts carry large records

	s := newStats()
	tick := time.NewTicker(statsEvery)
	defer tick.Stop()

	msgs := make(chan []byte, 256)
	readErr := make(chan error, 1)
	go func() {
		for {
			_, raw, err := conn.Read(ctx)
			if err != nil {
				readErr <- err
				return
			}
			msgs <- raw
		}
	}()

	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "shutdown")
			return nil
		case err := <-readErr:
			return fmt.Errorf("read jetstream: %w", err)
		case <-tick.C:
			s.log(in.Log)
		case raw := <-msgs:
			e, err := event.Parse(raw)
			if err != nil {
				s.parseErrors++
				continue
			}
			s.lastTimeUS = e.TimeUS
			subject, ok := router.Match(in.Routes, e)
			if !ok {
				s.unrouted++
				continue
			}
			s.routed[subject]++
		}
	}
}

func subscribeURL(base string, collections []string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("jetstream url: %w", err)
	}
	q := u.Query()
	for _, c := range collections {
		q.Add("wantedCollections", c)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

type stats struct {
	routed      map[string]int
	unrouted    int
	parseErrors int
	lastTimeUS  int64
	since       time.Time
}

func newStats() *stats { return &stats{routed: map[string]int{}, since: time.Now()} }

// log writes one line for the interval and resets the counters.
func (s *stats) log(l *slog.Logger) {
	total := s.unrouted + s.parseErrors
	attrs := []any{}
	for subject, n := range s.routed {
		total += n
		attrs = append(attrs, subject, n)
	}
	secs := time.Since(s.since).Seconds()
	attrs = append(attrs,
		"unrouted", s.unrouted,
		"parse_errors", s.parseErrors,
		"events_per_sec", int(float64(total)/secs),
		"ingest_lag_ms", time.Since(time.UnixMicro(s.lastTimeUS)).Milliseconds(),
	)
	l.Info("ingest stats", attrs...)
	*s = stats{routed: map[string]int{}, since: time.Now(), lastTimeUS: s.lastTimeUS}
}
