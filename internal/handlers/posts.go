package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"bskyrouter/internal/event"
	"bskyrouter/internal/notify"
	"bskyrouter/internal/worker"
)

// TombstoneChecker is what the posts path needs from the tombstone store.
type TombstoneChecker interface {
	Exists(ctx context.Context, uri string) (bool, error)
}

// Posts is the content path: a new post matches if its text contains any
// configured keyword as a whole word (or phrase), or its langs include any
// configured language. A match raises an alert with the post's URI, author and
// the rule, never the text.
type Posts struct {
	Keywords   []string // lowercase
	Languages  []string // lowercase
	Tombstones TombstoneChecker
	Notifier   notify.Notifier

	matched, skippedDeleted, tombstoneErrors atomic.Int64
}

type postRecord struct {
	Text  string   `json:"text"`
	Langs []string `json:"langs"`
}

func (p *Posts) Handle(ctx context.Context, e event.Event) error {
	if e.Commit == nil || e.Commit.RKey == "" {
		return worker.Permanent(errors.New("post event without a commit or rkey"))
	}
	var rec postRecord
	if err := json.Unmarshal(e.Commit.Record, &rec); err != nil {
		return worker.Permanent(fmt.Errorf("post record: %w", err))
	}
	rule, ok := p.match(rec)
	if !ok {
		return nil
	}

	// Checked only on a match: non-matching posts never alert, so checking them
	// would change nothing. If the store is unreachable, alert anyway rather
	// than block the path.
	deleted, err := p.Tombstones.Exists(ctx, e.URI())
	if err != nil {
		p.tombstoneErrors.Add(1)
	} else if deleted {
		p.skippedDeleted.Add(1)
		return nil
	}

	p.matched.Add(1)
	return p.Notifier.Notify(ctx, notify.Alert{
		ID:   "posts:" + e.URI() + ":" + rule,
		Type: "post_match",
		Attrs: []any{
			"uri", e.URI(),
			"author", e.DID,
			"rule", rule,
			"posted_at", time.UnixMicro(e.TimeUS).UTC().Format(time.RFC3339),
		},
	})
}

// match returns the first rule that matches: keywords are checked before
// languages.
func (p *Posts) match(rec postRecord) (rule string, ok bool) {
	if len(p.Keywords) > 0 {
		text := words(rec.Text)
		for _, k := range p.Keywords {
			if strings.Contains(text, words(k)) {
				return "keyword:" + k, true
			}
		}
	}
	for _, l := range rec.Langs {
		if slices.Contains(p.Languages, strings.ToLower(l)) {
			return "language:" + strings.ToLower(l), true
		}
	}
	return "", false
}

// words lowercases s and reduces it to its words, each wrapped in spaces, so a
// keyword matches whole words only: "nats" matches "I use NATS!" but not
// "senats". Multi-word keywords match as phrases.
func words(s string) string {
	f := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	return " " + strings.Join(f, " ") + " "
}

func (p *Posts) StatsAttrs() []any {
	return []any{
		"matched", p.matched.Swap(0),
		"skipped_deleted", p.skippedDeleted.Swap(0),
		"tombstone_errors", p.tombstoneErrors.Swap(0),
	}
}
