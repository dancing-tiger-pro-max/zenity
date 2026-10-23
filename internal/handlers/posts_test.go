package handlers

import (
	"context"
	"strings"
	"testing"

	"bskyrouter/internal/event"
	"bskyrouter/internal/notify"
)

type fakeTombstones map[string]bool

func (f fakeTombstones) Exists(_ context.Context, uri string) (bool, error) { return f[uri], nil }

type recorder struct{ alerts []notify.Alert }

func (r *recorder) Notify(_ context.Context, a notify.Alert) error {
	r.alerts = append(r.alerts, a)
	return nil
}

func post(rkey, text string, langs ...string) event.Event {
	rec := `{"text":"` + text + `","langs":["` + strings.Join(langs, `","`) + `"]}`
	return event.Event{DID: "did:plc:a", Kind: "commit", Commit: &event.Commit{
		Operation: "create", Collection: "app.bsky.feed.post", RKey: rkey, Record: []byte(rec)}}
}

func TestPosts(t *testing.T) {
	deleted := post("gone", "I love Golang")
	tests := []struct {
		name     string
		ev       event.Event
		wantRule string // "" = no alert
	}{
		{"keyword, any case", post("1", "I love Golang", "en"), "keyword:golang"},
		{"language", post("2", "bonjour", "fr"), "language:fr"},
		{"keyword wins over language", post("3", "golang en français", "fr"), "keyword:golang"},
		{"no match", post("4", "hello world", "en"), ""},
		{"whole words only", post("5", "the golangers met", "en"), ""},
		{"word next to punctuation", post("6", "Hello, Golang!", "en"), "keyword:golang"},
		{"phrase", post("7", "we love event streams", "en"), "keyword:event streams"},
		{"deleted before we saw it", deleted, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &recorder{}
			p := &Posts{Keywords: []string{"golang", "event streams"}, Languages: []string{"fr"},
				Tombstones: fakeTombstones{deleted.URI(): true}, Notifier: r}
			if err := p.Handle(context.Background(), tt.ev); err != nil {
				t.Fatal(err)
			}
			if tt.wantRule == "" {
				if len(r.alerts) != 0 {
					t.Fatalf("got alert %+v, want none", r.alerts[0])
				}
				return
			}
			if len(r.alerts) != 1 {
				t.Fatalf("got %d alerts, want 1", len(r.alerts))
			}
			a := r.alerts[0]
			if a.ID != "posts:"+tt.ev.URI()+":"+tt.wantRule {
				t.Errorf("alert ID = %q", a.ID)
			}
			for _, v := range a.Attrs {
				if s, ok := v.(string); ok && strings.Contains(strings.ToLower(s), "golang") && !strings.HasPrefix(s, "keyword:") {
					t.Errorf("alert leaks post text: %q", s)
				}
			}
		})
	}
}
