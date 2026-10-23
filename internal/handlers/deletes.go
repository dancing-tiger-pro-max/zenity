// Package handlers holds each path's work. Delivery (ack, retry, dead-letter)
// is the worker's job; a handler only does the work for one event.
package handlers

import (
	"context"
	"errors"
	"sync"

	"bskyrouter/internal/event"
	"bskyrouter/internal/worker"
)

// TombstoneWriter is what the deletes path needs from the tombstone store.
type TombstoneWriter interface {
	Put(ctx context.Context, uri, collection string, timeUS int64) error
}

// Deletes is the retraction path: every deletion, of any collection, gets a
// tombstone. Today only the posts path reads them; the rest exist for future
// readers and expire with the bucket's TTL.
type Deletes struct {
	Tombstones TombstoneWriter

	mu           sync.Mutex
	byCollection map[string]int
}

func (d *Deletes) Handle(ctx context.Context, e event.Event) error {
	if e.Commit == nil || e.Commit.RKey == "" {
		return worker.Permanent(errors.New("delete event without a commit or rkey"))
	}
	if err := d.Tombstones.Put(ctx, e.URI(), e.Collection(), e.TimeUS); err != nil {
		return err // retried by the worker
	}
	d.mu.Lock()
	if d.byCollection == nil {
		d.byCollection = map[string]int{}
	}
	d.byCollection[e.Collection()]++
	d.mu.Unlock()
	return nil
}

// StatsAttrs reports tombstones written per collection since the last call.
func (d *Deletes) StatsAttrs() []any {
	d.mu.Lock()
	defer d.mu.Unlock()
	attrs := []any{}
	for c, n := range d.byCollection {
		attrs = append(attrs, c, n)
	}
	d.byCollection = nil
	return attrs
}
