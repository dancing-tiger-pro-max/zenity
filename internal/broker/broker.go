// Package broker connects to NATS and creates the streams and KV buckets the
// roles share. Every role calls Ensure at startup; it's idempotent, so pods can
// start in any order.
package broker

import (
	"context"
	"fmt"
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

// Connect opens a NATS connection that keeps reconnecting on its own.
func Connect(url, name string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url,
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats %s: %w", url, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream: %w", err)
	}
	return nc, js, nil
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
