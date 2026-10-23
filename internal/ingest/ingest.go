// Package ingest holds the single Jetstream connection, routes each event and
// publishes it to NATS. In dry-run mode it only counts where events would go.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"bskyrouter/internal/event"
	"bskyrouter/internal/router"
)

const statsEvery = 5 * time.Second

// CursorStore persists the resume point (NATS KV in production).
type CursorStore interface {
	Load(ctx context.Context) (timeUS int64, ok bool, err error)
	Save(ctx context.Context, timeUS int64) error
}

type Ingester struct {
	URL            string
	Routes         []router.Route
	JS             jetstream.JetStream // nil in dry-run
	Cursor         CursorStore         // nil in dry-run: nothing is saved
	MaxPending     int
	CursorInterval time.Duration
	Backoff        []time.Duration // waits between reconnects; the last one repeats
	GapThreshold   time.Duration
	DryRun         bool
	Log            *slog.Logger

	// confirmedUS is the time_us of the newest event for which it and every
	// earlier event is either stored in NATS or deliberately dropped. It's the
	// only safe resume point.
	confirmedUS atomic.Int64
	stats       stats
}

// inflight is one event waiting for its publish ack. fut is nil for events
// that weren't published (unrouted, or dry-run): they're done immediately, but
// still queued so the confirmed point only moves in order.
type inflight struct {
	timeUS int64
	fut    jetstream.PubAckFuture
}

// Run resumes from the saved cursor and keeps a Jetstream session going until
// ctx is cancelled. When a session fails (Jetstream drops us, or NATS stops
// confirming), it waits and reconnects from the last confirmed event; anything
// unconfirmed is sent again and deduplicated by NATS.
func (in *Ingester) Run(ctx context.Context) error {
	if in.Cursor != nil {
		saved, ok, err := in.Cursor.Load(ctx)
		if err != nil {
			return err
		}
		if ok {
			in.confirmedUS.Store(saved)
			in.Log.Info("resuming from saved cursor", "cursor", saved, "behind", time.Since(time.UnixMicro(saved)).Round(time.Second).String())
		} else {
			in.Log.Info("no saved cursor, starting from now")
		}
		saverDone := make(chan struct{})
		go func() { defer close(saverDone); in.saveCursor(ctx) }()
		defer func() { <-saverDone }()
	}

	for attempt := 0; ; attempt++ {
		err := in.session(ctx, in.confirmedUS.Load())
		if ctx.Err() != nil {
			return nil
		}
		wait := in.Backoff[min(attempt, len(in.Backoff)-1)]
		in.Log.Warn("session ended, reconnecting from cursor", "err", err, "wait", wait.String(), "cursor", in.confirmedUS.Load())
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil
		}
		if err == nil {
			attempt = -1 // a clean session resets the backoff
		}
	}
}

// saveCursor writes the confirmed point every CursorInterval if it moved, and
// once more on shutdown.
func (in *Ingester) saveCursor(ctx context.Context) {
	tick := time.NewTicker(in.CursorInterval)
	defer tick.Stop()
	var saved int64
	save := func(ctx context.Context) {
		c := in.confirmedUS.Load()
		if c == 0 || c == saved {
			return
		}
		if err := in.Cursor.Save(ctx, c); err != nil {
			in.Log.Warn("cursor not saved", "err", err)
			return
		}
		saved = c
	}
	for {
		select {
		case <-tick.C:
			save(ctx)
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			save(final)
			cancel()
			in.Log.Info("cursor saved on shutdown", "cursor", saved)
			return
		}
	}
}

// session runs one Jetstream connection, starting at fromUS (0 = live). It
// returns when ctx is cancelled (nil) or the connection or a publish fails.
func (in *Ingester) session(ctx context.Context, fromUS int64) error {
	u, err := subscribeURL(in.URL, router.WantedCollections(in.Routes), fromUS)
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

	in.stats.reset()
	tick := time.NewTicker(statsEvery)
	defer tick.Stop()

	// done stops the helper goroutines when Run returns, so no send below can
	// block forever.
	done := make(chan struct{})
	defer close(done)

	msgs := make(chan []byte, in.MaxPending)
	readErr := make(chan error, 1)
	go func() {
		for {
			_, raw, err := conn.Read(ctx)
			if err != nil {
				readErr <- err
				return
			}
			select {
			case msgs <- raw:
			case <-done:
				return
			}
		}
	}()

	// The acker drains acks in publish order. Its queue is bounded, and the
	// NATS client caps unconfirmed publishes at MaxPending: if NATS stops
	// answering, publishing blocks and we stop reading from Jetstream. A publish
	// not confirmed within the publish timeout fails, and Run returns.
	queue := make(chan inflight, in.MaxPending)
	ackErr := make(chan error, 1)
	go in.ack(queue, ackErr, done)

	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "shutdown")
			return nil
		case err := <-readErr:
			return fmt.Errorf("read jetstream: %w", err)
		case err := <-ackErr:
			return fmt.Errorf("publish not confirmed: %w", err)
		case <-tick.C:
			in.logStats()
		case raw := <-msgs:
			item, err := in.handle(ctx, raw)
			if err != nil {
				return err
			}
			if fromUS > 0 && item.timeUS > 0 {
				in.checkGap(fromUS, item.timeUS)
				fromUS = 0 // only the first event after a resume
			}
			select {
			case queue <- item:
			case <-ctx.Done():
				return nil
			case err := <-ackErr:
				return fmt.Errorf("publish not confirmed: %w", err)
			}
		}
	}
}

// handle parses, routes and (unless dry-run) publishes one raw event.
func (in *Ingester) handle(ctx context.Context, raw []byte) (inflight, error) {
	e, err := event.Parse(raw)
	if err != nil {
		in.stats.parseErrors.Add(1)
		return inflight{}, nil // timeUS 0: doesn't move the confirmed point
	}
	in.stats.lastSeenUS.Store(e.TimeUS)

	subject, ok := router.Match(in.Routes, e)
	if !ok {
		in.stats.unrouted.Add(1)
		return inflight{timeUS: e.TimeUS}, nil
	}
	in.stats.routed(subject)
	if in.DryRun {
		return inflight{timeUS: e.TimeUS}, nil
	}

	msg := &nats.Msg{Subject: subject, Data: raw}
	fut, err := in.JS.PublishMsgAsync(msg, jetstream.WithMsgID(e.MsgID()))
	if errors.Is(err, nats.ErrMaxPayload) {
		// Failing here would retry the same event forever after every restart.
		// Skip it, but never silently.
		in.stats.oversized.Add(1)
		in.Log.Warn("event too large for NATS, skipped", "uri", e.URI(), "subject", subject, "bytes", len(raw))
		return inflight{timeUS: e.TimeUS}, nil
	}
	if err != nil {
		return inflight{}, fmt.Errorf("publish %s: %w", subject, err)
	}
	return inflight{timeUS: e.TimeUS, fut: fut}, nil
}

// ack waits for each publish ack in order and advances the confirmed point.
func (in *Ingester) ack(queue <-chan inflight, errs chan<- error, done <-chan struct{}) {
	for {
		var item inflight
		select {
		case item = <-queue:
		case <-done:
			return
		}
		if item.fut != nil {
			select {
			case ack := <-item.fut.Ok():
				if ack.Duplicate {
					in.stats.duplicates.Add(1)
				} else {
					in.stats.stored.Add(1)
				}
			case err := <-item.fut.Err():
				errs <- err
				return
			case <-done:
				return
			}
		}
		if item.timeUS > 0 {
			in.confirmedUS.Store(item.timeUS)
		}
	}
}

// ConfirmedUS is the newest safe resume point (0 until the first event is confirmed).
func (in *Ingester) ConfirmedUS() int64 { return in.confirmedUS.Load() }

// checkGap reports a resume that skipped history: Jetstream silently clamps a
// cursor older than its replay window (~36h) instead of failing.
func (in *Ingester) checkGap(cursorUS, firstUS int64) {
	jump := time.Duration(firstUS-cursorUS) * time.Microsecond
	if jump > in.GapThreshold {
		in.Log.Warn("gap detected: events between cursor and first received event are lost",
			"cursor", cursorUS, "first_event", firstUS, "gap", jump.Round(time.Second).String())
	}
}

func subscribeURL(base string, collections []string, cursorUS int64) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("jetstream url: %w", err)
	}
	q := u.Query()
	for _, c := range collections {
		q.Add("wantedCollections", c)
	}
	if cursorUS > 0 {
		q.Set("cursor", strconv.FormatInt(cursorUS, 10)) // inclusive: that event comes again
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
