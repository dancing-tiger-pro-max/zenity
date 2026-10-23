// Package worker is the delivery loop every path shares: pull a message, apply
// the slow-path delay, hand the event to the path's Handler, then ack, retry
// with backoff, or move it to the dead-letter stream. A path only implements
// Handle.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"bskyrouter/internal/event"
)

// Handler does one path's work for one event. Returning an error retries the
// event later; wrap it with Permanent when retrying can't help.
type Handler interface {
	Handle(ctx context.Context, e event.Event) error
}

// StatsReporter is optionally implemented by a Handler to add path-specific
// counters to the periodic stats line.
type StatsReporter interface {
	StatsAttrs() []any
}

type permanentError struct{ error }

func (p permanentError) Unwrap() error { return p.error }

// Permanent marks an error that retrying can't fix, so the event goes straight
// to the dead-letter stream.
func Permanent(err error) error { return permanentError{err} }

type Worker struct {
	Name         string // path name: consumer name and dlq.<name> subject
	Stream       string
	Handler      Handler
	JS           jetstream.JetStream
	AckWait      time.Duration
	MaxDeliver   int
	Backoff      []time.Duration // delay before each retry; the last one repeats
	ProcessDelay time.Duration   // slow-path demo
	Log          *slog.Logger

	processed, retried, deadLettered atomic.Int64
	lastEventUS                      atomic.Int64
}

const statsEvery = 10 * time.Second

// Run consumes the path's stream through its durable pull consumer until ctx
// is cancelled. Replicas of the same path share the consumer, so NATS spreads
// messages between them.
func (w *Worker) Run(ctx context.Context) error {
	cons, err := w.JS.CreateOrUpdateConsumer(ctx, w.Stream, jetstream.ConsumerConfig{
		Durable:       w.Name,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       w.AckWait,
		MaxDeliver:    w.MaxDeliver,
		MaxAckPending: 256,
	})
	if err != nil {
		return fmt.Errorf("consumer %s on %s: %w", w.Name, w.Stream, err)
	}
	iter, err := cons.Messages(jetstream.PullMaxMessages(64))
	if err != nil {
		return fmt.Errorf("consume %s: %w", w.Stream, err)
	}
	go func() {
		<-ctx.Done()
		iter.Stop() // unblocks Next
	}()
	go w.logStats(ctx)

	w.Log.Info("consuming", "stream", w.Stream, "consumer", w.Name, "process_delay", w.ProcessDelay.String())
	for {
		msg, err := iter.Next()
		if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			return nil
		}
		if err != nil {
			// e.g. NATS unreachable: the client reconnects by itself.
			w.Log.Warn("fetch failed", "err", err)
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}
		if !sleep(ctx, w.ProcessDelay) {
			return nil // shutting down: the unacked message is redelivered later
		}
		w.process(ctx, msg)
	}
}

func (w *Worker) process(ctx context.Context, msg jetstream.Msg) {
	err := w.handle(ctx, msg.Data())
	if err == nil {
		w.processed.Add(1)
		if err := msg.Ack(); err != nil {
			w.Log.Warn("ack failed, message will be redelivered", "err", err)
		}
		return
	}

	attempt := uint64(1)
	if md, mdErr := msg.Metadata(); mdErr == nil {
		attempt = md.NumDelivered
	}
	var permanent permanentError
	if errors.As(err, &permanent) || attempt >= uint64(w.MaxDeliver) {
		w.deadLetter(ctx, msg, err, attempt)
		return
	}
	delay := w.Backoff[min(int(attempt)-1, len(w.Backoff)-1)]
	w.retried.Add(1)
	w.Log.Warn("handler failed, retrying", "err", err, "attempt", attempt, "retry_in", delay.String())
	if err := msg.NakWithDelay(delay); err != nil {
		w.Log.Warn("nak failed, message will be redelivered after ack wait", "err", err)
	}
}

// handle parses and runs the handler; a panic becomes an error, so one bad
// event fails an attempt instead of killing the pod.
func (w *Worker) handle(ctx context.Context, raw []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	e, err := event.Parse(raw)
	if err != nil {
		return Permanent(err)
	}
	w.lastEventUS.Store(e.TimeUS)
	return w.Handler.Handle(ctx, e)
}

// deadLetter parks the message in the DLQ stream with why it failed, then acks
// the original. If the DLQ publish fails, the original is retried instead, so
// nothing is dropped.
func (w *Worker) deadLetter(ctx context.Context, msg jetstream.Msg, cause error, attempts uint64) {
	dl := &nats.Msg{Subject: "dlq." + w.Name, Data: msg.Data(), Header: nats.Header{}}
	dl.Header.Set("Original-Subject", msg.Subject())
	dl.Header.Set("Error", cause.Error())
	dl.Header.Set("Attempts", strconv.FormatUint(attempts, 10))

	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := w.JS.PublishMsg(pctx, dl); err != nil {
		w.Log.Error("dead-letter publish failed, will retry the message", "err", err)
		msg.NakWithDelay(w.Backoff[len(w.Backoff)-1])
		return
	}
	w.deadLettered.Add(1)
	w.Log.Error("moved to dead-letter stream", "subject", dl.Subject, "err", cause, "attempts", attempts)
	if err := msg.Ack(); err != nil {
		w.Log.Warn("ack after dead-letter failed", "err", err)
	}
}

func (w *Worker) logStats(ctx context.Context) {
	tick := time.NewTicker(statsEvery)
	defer tick.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			n := w.processed.Swap(0)
			attrs := []any{
				"processed", n,
				"per_sec", int(float64(n) / now.Sub(last).Seconds()),
				"retried", w.retried.Swap(0),
				"dead_lettered", w.deadLettered.Swap(0),
				"event_lag_ms", msSince(w.lastEventUS.Load()),
			}
			if r, ok := w.Handler.(StatsReporter); ok {
				attrs = append(attrs, r.StatsAttrs()...)
			}
			w.Log.Info("worker stats", attrs...)
			last = now
		}
	}
}

// sleep waits d, or returns false if ctx ends first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func msSince(timeUS int64) int64 {
	if timeUS == 0 {
		return -1
	}
	return time.Since(time.UnixMicro(timeUS)).Milliseconds()
}
