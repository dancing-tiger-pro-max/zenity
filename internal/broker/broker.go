// Package broker connects to NATS and creates the streams and KV buckets the
// roles share. Every role calls Ensure at startup; it's idempotent, so pods can
// start in any order.
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"bskyrouter/internal/config"
)

// Stream names. Which subjects each one captures is defined in streamConfigs.
const (
	StreamPosts   = "POSTS"
	StreamLikes   = "LIKES"
	StreamDeletes = "DELETES"
	StreamDLQ     = "DLQ"
)

// KV bucket names.
const (
	BucketCursor     = "cursor"
	BucketTombstones = "tombstones"
)

// Connect opens a NATS connection that keeps reconnecting on its own. If NATS
// isn't reachable yet, it keeps trying until ctx ends instead of failing: on a
// fresh cluster the Service address can take a few seconds to work after the
// NATS pod is ready, and exiting would put the pod in Kubernetes' growing
// restart backoff (CrashLoopBackOff), which made `make up` look stuck.
func Connect(ctx context.Context, url, name string, log *slog.Logger, opts ...jetstream.JetStreamOpt) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url,
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.RetryOnFailedConnect(true),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats %s: %w", url, err)
	}
	if err := waitConnected(ctx, nc, url, log); err != nil {
		nc.Close()
		return nil, nil, err
	}
	js, err := jetstream.New(nc, opts...)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream: %w", err)
	}
	return nc, js, nil
}

func waitConnected(ctx context.Context, nc *nats.Conn, url string, log *slog.Logger) error {
	start := time.Now()
	lastLog := start
	for !nc.IsConnected() {
		if time.Since(lastLog) >= 5*time.Second {
			log.Warn("waiting for NATS", "url", url, "waited", time.Since(start).Round(time.Second).String())
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("connect nats %s: %w", url, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	if time.Since(start) > time.Second {
		log.Info("connected to NATS", "after", time.Since(start).Round(time.Second).String())
	}
	return nil
}

// ConnectPublisher connects for the ingester. The async publish timeout is
// essential: without it, a publish to an unreachable NATS never resolves and
// the ingester freezes silently instead of failing and resuming from its cursor.
func ConnectPublisher(ctx context.Context, url string, maxPending int, publishTimeout time.Duration, log *slog.Logger) (*nats.Conn, jetstream.JetStream, error) {
	return Connect(ctx, url, "ingester", log,
		jetstream.WithPublishAsyncMaxPending(maxPending),
		jetstream.WithPublishAsyncTimeout(publishTimeout))
}

// Ensure creates or updates every stream and KV bucket to match cfg.
func Ensure(ctx context.Context, js jetstream.JetStream, cfg config.Config) error {
	for _, sc := range streamConfigs(cfg) {
		if _, err := js.CreateOrUpdateStream(ctx, sc); err != nil {
			return fmt.Errorf("stream %s: %w", sc.Name, err)
		}
	}
	buckets := []jetstream.KeyValueConfig{
		{Bucket: BucketCursor, History: 1, Storage: jetstream.FileStorage},
		{Bucket: BucketTombstones, History: 1, Storage: jetstream.FileStorage, TTL: cfg.Workers.Deletes.TombstoneTTL},
	}
	for _, kc := range buckets {
		if _, err := js.CreateOrUpdateKeyValue(ctx, kc); err != nil {
			return fmt.Errorf("kv %s: %w", kc.Bucket, err)
		}
	}
	return nil
}

// streamConfigs: one stream per path. Limits retention keeps processed messages
// (the likes worker replays its window after a restart), and DiscardOld means a
// full stream evicts its own oldest message instead of refusing the publish, so
// the ingester never waits on a slow path.
func streamConfigs(cfg config.Config) []jetstream.StreamConfig {
	s := cfg.Streams
	path := func(name string, limits config.StreamLimits, subjects ...string) jetstream.StreamConfig {
		return jetstream.StreamConfig{
			Name:       name,
			Subjects:   subjects,
			Retention:  jetstream.LimitsPolicy,
			Discard:    jetstream.DiscardOld,
			Storage:    jetstream.FileStorage,
			MaxAge:     limits.MaxAge,
			MaxBytes:   int64(limits.MaxBytes),
			Duplicates: s.DuplicatesWindow,
		}
	}
	return []jetstream.StreamConfig{
		path(StreamPosts, s.Posts, "bsky.post.create"),
		path(StreamLikes, s.Likes, "bsky.like.create", "bsky.repost.create"),
		path(StreamDeletes, s.Deletes, "bsky.delete"),
		path(StreamDLQ, s.DLQ, "dlq.>"),
	}
}
