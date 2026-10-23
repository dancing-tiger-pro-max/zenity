// Package event parses Jetstream events. It decodes only what routing and the
// handlers need; the record body stays raw until a handler asks for it.
package event

import (
	"encoding/json"
	"fmt"
)

// Event is one Jetstream message.
type Event struct {
	DID    string  `json:"did"`
	TimeUS int64   `json:"time_us"`
	Kind   string  `json:"kind"` // commit | identity | account
	Commit *Commit `json:"commit,omitempty"`
}

// Commit is present only when Kind is "commit".
type Commit struct {
	Operation  string          `json:"operation"`  // create | update | delete
	Collection string          `json:"collection"` // e.g. app.bsky.feed.post
	RKey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record,omitempty"` // absent on deletes
}

// Parse decodes a raw Jetstream message.
func Parse(raw []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return Event{}, fmt.Errorf("parse event: %w", err)
	}
	if e.Kind == "" {
		return Event{}, fmt.Errorf("parse event: missing kind")
	}
	return e, nil
}

// Collection returns the commit collection, or "" for non-commit events.
func (e Event) Collection() string {
	if e.Commit == nil {
		return ""
	}
	return e.Commit.Collection
}

// Operation returns the commit operation, or "" for non-commit events.
func (e Event) Operation() string {
	if e.Commit == nil {
		return ""
	}
	return e.Commit.Operation
}

// URI is the AT URI of the record this commit touches: at://<did>/<collection>/<rkey>.
func (e Event) URI() string {
	if e.Commit == nil {
		return ""
	}
	return fmt.Sprintf("at://%s/%s/%s", e.DID, e.Commit.Collection, e.Commit.RKey)
}

// MsgID identifies the event for broker-side deduplication.
func (e Event) MsgID() string {
	return fmt.Sprintf("%s:%d", e.DID, e.TimeUS)
}
