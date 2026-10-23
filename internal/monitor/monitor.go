package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"bskyrouter/internal/notify"
)

// Path is one stream and the durable consumer its workers share.
type Path struct {
	Stream, Consumer string
}

type Monitor struct {
	JS        jetstream.JetStream
	Paths     []Path
	Interval  time.Duration
	FullRatio float64
	Backlog   float64
	Notifier  notify.Notifier
	Log       *slog.Logger

	state map[string]*pathState
}

// statusEvery: a status line every few checks, so the log shows the picture
// without repeating it every few seconds.
const statusEvery = 6

func (m *Monitor) Run(ctx context.Context) error {
	m.state = map[string]*pathState{}
	tick := time.NewTicker(m.Interval)
	defer tick.Stop()
	m.Log.Info("monitoring", "paths", len(m.Paths), "every", m.Interval.String())
	for n := 0; ; n++ {
		status := []any{}
		for _, p := range m.Paths {
			s, ok, err := m.snapshot(ctx, p)
			if err != nil {
				if ctx.Err() == nil {
					m.Log.Warn("check failed", "stream", p.Stream, "err", err)
				}
				continue
			}
			if !ok {
				status = append(status, p.Stream, "no consumer yet")
				continue
			}
			m.report(ctx, p, s)
			status = append(status, p.Stream, fmt.Sprintf("%d waiting (%.0f%% unread)", s.Pending, 100*unread(s)))
		}
		if n%statusEvery == 0 {
			m.Log.Info("status", status...)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (m *Monitor) snapshot(ctx context.Context, p Path) (Snapshot, bool, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := m.JS.Stream(cctx, p.Stream)
	if err != nil {
		return Snapshot{}, false, err
	}
	si, err := stream.Info(cctx)
	if err != nil {
		return Snapshot{}, false, err
	}
	cons, err := stream.Consumer(cctx, p.Consumer)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return Snapshot{}, false, nil // that path's worker has never run
	}
	if err != nil {
		return Snapshot{}, false, err
	}
	ci, err := cons.Info(cctx)
	if err != nil {
		return Snapshot{}, false, err
	}
	return Snapshot{
		Msgs: si.State.Msgs, Bytes: si.State.Bytes,
		FirstSeq: si.State.FirstSeq, FirstTime: si.State.FirstTime,
		MaxBytes: si.Config.MaxBytes, MaxAge: si.Config.MaxAge,
		Pending: ci.NumPending, DeliveredSeq: ci.Delivered.Stream, Deliveries: ci.Delivered.Consumer,
		Now: time.Now(), FullRatio: m.FullRatio, Backlog: m.Backlog,
	}, true, nil
}

func (m *Monitor) report(ctx context.Context, p Path, s Snapshot) {
	st := m.state[p.Stream]
	if st == nil {
		// Start from what's already gone, so a restart doesn't re-report old loss.
		st = &pathState{lostUpTo: max(s.FirstSeq, 1) - 1}
		m.state[p.Stream] = st
	}
	a := assess(s, st)
	if a.StartWarning {
		m.notify(ctx, notify.Alert{
			ID:   fmt.Sprintf("backlog:%s:%d", p.Stream, s.FirstSeq),
			Type: "backlog_warning",
			Attrs: []any{"stream", p.Stream, "waiting", s.Pending, "stored", s.Msgs,
				"unread_pct", int(100 * a.Unread), "detail", "stream near its limit and mostly unread: unread messages will be evicted if the worker doesn't catch up"},
		})
	}
	if a.EndWarning {
		m.notify(ctx, notify.Alert{
			ID:    fmt.Sprintf("backlog-ok:%s:%d", p.Stream, s.FirstSeq),
			Type:  "backlog_ok",
			Attrs: []any{"stream", p.Stream, "waiting", s.Pending},
		})
	}
	if a.LostCount > 0 {
		m.notify(ctx, notify.Alert{
			ID:   fmt.Sprintf("lost:%s:%d-%d", p.Stream, a.LostFrom, a.LostTo),
			Type: "events_lost",
			Attrs: []any{"stream", p.Stream, "lost", a.LostCount, "from_seq", a.LostFrom, "to_seq", a.LostTo,
				"detail", "evicted before the worker read them"},
		})
	}
}

func (m *Monitor) notify(ctx context.Context, a notify.Alert) {
	if err := m.Notifier.Notify(ctx, a); err != nil {
		m.Log.Warn("notify failed", "alert", a.ID, "err", err)
	}
}

func unread(s Snapshot) float64 {
	if s.Msgs == 0 {
		return 0
	}
	return float64(s.Pending) / float64(s.Msgs)
}
