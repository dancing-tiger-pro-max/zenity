package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"bskyrouter/internal/engagement"
	"bskyrouter/internal/event"
	"bskyrouter/internal/notify"
	"bskyrouter/internal/worker"
)

// Likes is the engagement path: likes and reposts add to one rolling count per
// target post; crossing the threshold raises one alert per burst. The counts
// live in memory, so this path runs as a single replica, and on startup it
// rebuilds them by re-reading one window from the stream (Prepare).
type Likes struct {
	Window   *engagement.Window
	Notifier notify.Notifier
	Log      *slog.Logger

	mu        sync.Mutex // Window isn't concurrency-safe; stats read it too
	replaying bool       // rebuild counts without alerting
	alerts    int
}

type subjectRecord struct {
	Subject struct {
		URI string `json:"uri"`
	} `json:"subject"`
}

func (l *Likes) Handle(ctx context.Context, e event.Event) error {
	if e.Commit == nil || e.Commit.RKey == "" {
		return worker.Permanent(errors.New("like event without a commit or rkey"))
	}
	kind := engagement.Like
	if e.Commit.Collection == "app.bsky.feed.repost" {
		kind = engagement.Repost
	}
	var rec subjectRecord
	if err := json.Unmarshal(e.Commit.Record, &rec); err != nil || rec.Subject.URI == "" {
		return worker.Permanent(fmt.Errorf("like/repost without a subject: %v", err))
	}

	l.mu.Lock()
	c, crossed := l.Window.Add(e.URI(), rec.Subject.URI, kind, e.TimeUS)
	notifyNow := crossed && !l.replaying
	if notifyNow {
		l.alerts++
	}
	l.mu.Unlock()
	if !notifyNow {
		return nil
	}

	return l.Notifier.Notify(ctx, notify.Alert{
		ID:   fmt.Sprintf("engagement:%s:%d", c.Target, c.AtUS),
		Type: "engagement",
		Attrs: []any{
			"target", c.Target,
			"likes", c.Likes,
			"reposts", c.Reposts,
			"window", l.Window.Size.String(),
			"threshold", l.Window.Threshold,
			"at", time.UnixMicro(c.AtUS).UTC().Format(time.RFC3339),
		},
	})
}

// Prepare runs before the worker starts consuming. It re-reads the window of
// messages just before the consumer's ack floor (the last fully processed
// message) through a temporary ordered consumer, rebuilding the counts
// silently: alerts in that range were already sent before the restart.
// Overlap with redeliveries is harmless, since the window drops duplicates.
func (l *Likes) Prepare(ctx context.Context, js jetstream.JetStream, stream string, cons jetstream.Consumer) error {
	info, err := cons.Info(ctx)
	if err != nil {
		return fmt.Errorf("consumer info: %w", err)
	}
	floor := info.AckFloor.Stream
	if floor == 0 {
		l.Log.Info("first start, nothing to replay")
		return nil
	}
	s, err := js.Stream(ctx, stream)
	if err != nil {
		return err
	}
	last, err := s.GetMsg(ctx, floor)
	if err != nil {
		l.Log.Warn("last processed message no longer in the stream, starting with empty counts", "seq", floor, "err", err)
		return nil
	}
	// Stored time is when the ingester saved it, a little after the event
	// time; the margin covers that, and older events are ignored by the window.
	start := last.Time.Add(-l.Window.Size - 5*time.Second)
	replay, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &start,
	})
	if err != nil {
		return fmt.Errorf("replay consumer: %w", err)
	}

	l.mu.Lock()
	l.replaying = true
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.replaying = false
		l.mu.Unlock()
	}()

	// Read in batches; one request per message made a restart take ~45s.
	iter, err := replay.Messages(jetstream.PullMaxMessages(1000))
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	defer iter.Stop()

	began := time.Now()
	n := 0
	for {
		msg, err := iter.Next(jetstream.NextMaxWait(2 * time.Second))
		if errors.Is(err, jetstream.ErrMsgIteratorClosed) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			break // nothing more stored
		}
		if err != nil {
			return fmt.Errorf("replay: %w", err)
		}
		md, err := msg.Metadata()
		if err != nil {
			return err
		}
		if md.Sequence.Stream > floor {
			break // past what was processed: the durable consumer takes it from here
		}
		if e, err := event.Parse(msg.Data()); err == nil {
			l.Handle(ctx, e)
		}
		n++
	}
	l.mu.Lock()
	targets := l.Window.Targets()
	l.mu.Unlock()
	l.Log.Info("rebuilt counts from replay", "messages", n, "targets", targets,
		"from", start.UTC().Format(time.RFC3339), "took", time.Since(began).Round(time.Millisecond).String())
	return nil
}

func (l *Likes) StatsAttrs() []any {
	l.mu.Lock()
	defer l.mu.Unlock()
	a := l.alerts
	l.alerts = 0
	return []any{"alerts", a, "targets_tracked", l.Window.Targets()}
}
