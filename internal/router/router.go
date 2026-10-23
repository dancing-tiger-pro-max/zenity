// Package router maps an event's type to the NATS subject of the path that handles it.
package router

import "bskyrouter/internal/event"

// Route is one row of the routing table. An empty field matches anything.
type Route struct {
	Kind       string
	Collection string
	Operation  string
	Subject    string
}

// Table is checked top to bottom; the first match wins. Deletes come first so
// every deletion, of any collection, goes to the deletes path.
var Table = []Route{
	{Kind: "commit", Operation: "delete", Subject: "bsky.delete"},
	{Kind: "commit", Collection: "app.bsky.feed.post", Operation: "create", Subject: "bsky.post.create"},
	{Kind: "commit", Collection: "app.bsky.feed.like", Operation: "create", Subject: "bsky.like.create"},
	{Kind: "commit", Collection: "app.bsky.feed.repost", Operation: "create", Subject: "bsky.repost.create"},
}

// Match returns the subject for e, or ok=false if no route matches.
func Match(table []Route, e event.Event) (subject string, ok bool) {
	for _, r := range table {
		if matches(r.Kind, e.Kind) && matches(r.Collection, e.Collection()) && matches(r.Operation, e.Operation()) {
			return r.Subject, true
		}
	}
	return "", false
}

// WantedCollections builds Jetstream's server-side collection filter from the
// table. If any route accepts any collection, it returns nil: no filter.
func WantedCollections(table []Route) []string {
	var cols []string
	seen := map[string]bool{}
	for _, r := range table {
		if r.Collection == "" {
			return nil
		}
		if !seen[r.Collection] {
			seen[r.Collection] = true
			cols = append(cols, r.Collection)
		}
	}
	return cols
}

func matches(rule, value string) bool { return rule == "" || rule == value }
