package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"bskyrouter/internal/broker"
	"bskyrouter/internal/config"
	"bskyrouter/internal/event"
	"bskyrouter/internal/natstest"
)

// scripted fails by rkey: "flaky" fails twice then succeeds, "broken" always
// fails, "panics" panics; anything else succeeds.
type scripted struct {
	mu       sync.Mutex
	attempts map[string]int
	done     []string
}

func (s *scripted) Handle(_ context.Context, e event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rkey := e.Commit.RKey
	s.attempts[rkey]++
	switch {
	case rkey == "flaky" && s.attempts[rkey] <= 2:
		return errors.New("temporary failure")
	case rkey == "broken":
		return errors.New("always fails")
	case rkey == "panics":
		panic("handler bug")
	}
	s.done = append(s.done, rkey)
	return nil
}

func (s *scripted) seen(rkey string) (attempts int, done bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.done {
		if d == rkey {
			done = true
		}
	}
	return s.attempts[rkey], done
}

// One test for every delivery outcome: success, retry then success, retries
// exhausted, panic, and an unparseable message, while good messages keep
// flowing past the bad ones.
func TestDeliveryOutcomes(t *testing.T) {
	ns := natstest.Start(t)
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	nc, js, err := broker.Connect(context.Background(), ns.ClientURL(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := broker.Ensure(ctx, js, cfg); err != nil {
		t.Fatal(err)
	}

	h := &scripted{attempts: map[string]int{}}
	const maxDeliver = 3
	w := &Worker{
		Name: "posts", Stream: broker.StreamPosts, Handler: h, JS: js,
		AckWait: 5 * time.Second, MaxDeliver: maxDeliver,
		Backoff: []time.Duration{50 * time.Millisecond},
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	go w.Run(ctx)

	pub := func(data string) {
		if _, err := js.Publish(ctx, "bsky.post.create", []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	post := func(rkey string) string {
		return fmt.Sprintf(`{"did":"did:plc:t","time_us":1,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":%q}}`, rkey)
	}
	pub(post("ok-1"))
	pub(post("flaky"))
	pub(post("broken"))
	pub(post("panics"))
	pub("this is not json")
	pub(post("ok-2"))

	natstest.WaitFor(t, 10*time.Second, "all messages settled", func() bool {
		_, ok2 := h.seen("ok-2")
		_, flaky := h.seen("flaky")
		return ok2 && flaky && w.deadLettered.Load() == 3
	})

	// Retried, then succeeded.
	if n, done := h.seen("flaky"); n != 3 || !done {
		t.Errorf("flaky: %d attempts, done=%v; want 3 and done", n, done)
	}
	// Retries exhausted: exactly maxDeliver attempts, never more.
	if n, _ := h.seen("broken"); n != maxDeliver {
		t.Errorf("broken: %d attempts, want %d", n, maxDeliver)
	}
	// A panic is a failed attempt, not a dead worker.
	if n, _ := h.seen("panics"); n != maxDeliver {
		t.Errorf("panics: %d attempts, want %d", n, maxDeliver)
	}

	// The dead-letter stream holds the three bad messages, with why.
	dlq, err := js.Stream(ctx, broker.StreamDLQ)
	if err != nil {
		t.Fatal(err)
	}
	reasons := map[string]string{}
	for seq := uint64(1); seq <= 3; seq++ {
		m, err := dlq.GetMsg(ctx, seq)
		if err != nil {
			t.Fatalf("dlq message %d: %v", seq, err)
		}
		if m.Subject != "dlq.posts" || m.Header.Get("Original-Subject") != "bsky.post.create" {
			t.Errorf("dlq message %d: subject %q, original %q", seq, m.Subject, m.Header.Get("Original-Subject"))
		}
		reasons[m.Header.Get("Attempts")+" "+m.Header.Get("Error")] = string(m.Data)
	}
	for _, want := range []string{"3 always fails", "3 handler panic: handler bug"} {
		if _, ok := reasons[want]; !ok {
			t.Errorf("dlq missing %q; got %v", want, keys(reasons))
		}
	}
	// Unparseable: straight to the DLQ on the first attempt, since retrying can't help.
	found := false
	for r, data := range reasons {
		if data == "this is not json" && r[:2] == "1 " {
			found = true
		}
	}
	if !found {
		t.Errorf("garbage not dead-lettered on its first attempt; got %v", keys(reasons))
	}

	// Nothing left unacked: every message was settled one way or another.
	cons, err := js.Consumer(ctx, broker.StreamPosts, "posts")
	if err != nil {
		t.Fatal(err)
	}
	natstest.WaitFor(t, 5*time.Second, "consumer fully acked", func() bool {
		info, err := cons.Info(ctx)
		return err == nil && info.NumPending == 0 && info.NumAckPending == 0
	})
}

func keys(m map[string]string) []string {
	var k []string
	for key := range m {
		k = append(k, key)
	}
	return k
}
