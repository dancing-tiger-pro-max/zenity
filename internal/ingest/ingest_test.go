package ingest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"bskyrouter/internal/broker"
	"bskyrouter/internal/config"
	"bskyrouter/internal/router"
)

// Regression test for a freeze found live: with NATS gone, async publishes
// never resolved and the ingester hung silently, ignoring shutdown. It must
// instead fail within the publish timeout, so it can resume from its cursor.
//
// The feed is slow on purpose: with fewer unconfirmed publishes than the
// max-pending cap, the client's own stall guard never fires, so only the
// publish timeout can end the wait. That's the quiet-traffic case that froze.
func TestIngesterFailsFastWhenNATSDies(t *testing.T) {
	ns := startNATS(t)
	var paused atomic.Bool
	feed := fakeJetstream(t, &paused)

	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	const publishTimeout = time.Second
	const maxPending = 10_000
	nc, js, err := broker.ConnectPublisher(ns.ClientURL(), maxPending, publishTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if err := broker.Ensure(context.Background(), js, cfg); err != nil {
		t.Fatal(err)
	}

	in := &Ingester{
		URL:        feed,
		Routes:     router.Table,
		JS:         js,
		MaxPending: maxPending,
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	result := make(chan error, 1)
	go func() { result <- in.Run(context.Background()) }()

	waitFor(t, 5*time.Second, "events confirmed before NATS dies", func() bool { return in.ConfirmedUS() > 0 })

	// Reproduce the frozen case exactly: nothing in flight when NATS dies (so no
	// instant "no responders" error), and new publishes only once the client
	// knows it's disconnected, when it silently buffers them for a reconnect.
	paused.Store(true)
	time.Sleep(200 * time.Millisecond)
	ns.Shutdown()
	waitFor(t, 5*time.Second, "client to notice NATS is gone", func() bool { return nc.Status() != nats.CONNECTED })
	paused.Store(false)
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "publish not confirmed") {
			t.Fatalf("Run returned %v, want a publish-not-confirmed error", err)
		}
	case <-time.After(publishTimeout + 5*time.Second):
		t.Fatal("ingester still running after NATS died: it froze instead of failing")
	}
}

// startNATS runs a real NATS server with JetStream inside the test.
func startNATS(t *testing.T) *server.Server {
	t.Helper()
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(ns.Shutdown)
	return ns
}

// fakeJetstream serves a WebSocket that streams like events, like the real feed.
func fakeJetstream(t *testing.T, paused *atomic.Bool) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		base := time.Now().UnixMicro()
		for i := 0; ; i++ {
			for paused.Load() {
				time.Sleep(10 * time.Millisecond)
			}
			ev := fmt.Sprintf(`{"did":"did:plc:t","time_us":%d,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like","rkey":"r%d"}}`, base+int64(i), i)
			if err := c.Write(r.Context(), websocket.MessageText, []byte(ev)); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func waitFor(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
