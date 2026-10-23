package router

import (
	"slices"
	"testing"

	"bskyrouter/internal/event"
)

func commit(collection, op string) event.Event {
	return event.Event{DID: "did:plc:x", Kind: "commit", Commit: &event.Commit{Collection: collection, Operation: op}}
}

func TestMatch(t *testing.T) {
	tests := []struct {
		name    string
		ev      event.Event
		subject string // "" means no route
	}{
		{"post create", commit("app.bsky.feed.post", "create"), "bsky.post.create"},
		{"like create", commit("app.bsky.feed.like", "create"), "bsky.like.create"},
		{"repost create", commit("app.bsky.feed.repost", "create"), "bsky.repost.create"},

		// Deletes win over the collection rows, whatever the collection.
		{"post delete", commit("app.bsky.feed.post", "delete"), "bsky.delete"},
		{"like delete", commit("app.bsky.feed.like", "delete"), "bsky.delete"},
		{"follow delete", commit("app.bsky.graph.follow", "delete"), "bsky.delete"},

		// No route: dropped by the ingester.
		{"follow create", commit("app.bsky.graph.follow", "create"), ""},
		{"post update", commit("app.bsky.feed.post", "update"), ""},
		{"profile update", commit("app.bsky.actor.profile", "update"), ""},
		{"identity", event.Event{Kind: "identity"}, ""},
		{"account", event.Event{Kind: "account"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Match(Table, tt.ev)
			if ok != (tt.subject != "") || got != tt.subject {
				t.Errorf("Match = %q, %v; want %q", got, ok, tt.subject)
			}
		})
	}
}

func TestWantedCollections(t *testing.T) {
	// The delete route accepts any collection, so the server must send everything.
	if got := WantedCollections(Table); got != nil {
		t.Errorf("default table: got %v, want nil (unfiltered)", got)
	}

	narrowed := []Route{
		{Kind: "commit", Collection: "app.bsky.feed.post", Operation: "delete", Subject: "bsky.delete"},
		{Kind: "commit", Collection: "app.bsky.feed.post", Operation: "create", Subject: "bsky.post.create"},
		{Kind: "commit", Collection: "app.bsky.feed.like", Operation: "create", Subject: "bsky.like.create"},
	}
	want := []string{"app.bsky.feed.post", "app.bsky.feed.like"}
	if got := WantedCollections(narrowed); !slices.Equal(got, want) {
		t.Errorf("narrowed table: got %v, want %v", got, want)
	}
}
