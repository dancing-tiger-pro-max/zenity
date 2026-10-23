package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// Tombstones records deleted records in NATS KV so paths that see a create
// after its delete (they travel on separate streams) can skip it. Entries
// expire with the bucket's TTL.
type Tombstones struct {
	kv jetstream.KeyValue
}

type tombstone struct {
	URI        string `json:"uri"`
	Collection string `json:"collection"`
	TimeUS     int64  `json:"time_us"`
}

func NewTombstones(ctx context.Context, js jetstream.JetStream) (*Tombstones, error) {
	kv, err := js.KeyValue(ctx, BucketTombstones)
	if err != nil {
		return nil, fmt.Errorf("tombstones bucket: %w", err)
	}
	return &Tombstones{kv: kv}, nil
}

// Put records that uri was deleted. Writing the same tombstone twice is harmless.
func (t *Tombstones) Put(ctx context.Context, uri, collection string, timeUS int64) error {
	v, _ := json.Marshal(tombstone{URI: uri, Collection: collection, TimeUS: timeUS})
	if _, err := t.kv.Put(ctx, tombstoneKey(uri), v); err != nil {
		return fmt.Errorf("put tombstone: %w", err)
	}
	return nil
}

// Exists reports whether uri has a tombstone.
func (t *Tombstones) Exists(ctx context.Context, uri string) (bool, error) {
	_, err := t.kv.Get(ctx, tombstoneKey(uri))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get tombstone: %w", err)
	}
	return true, nil
}

// tombstoneKey hashes the URI: KV keys can't contain ':' and URIs do.
func tombstoneKey(uri string) string {
	sum := sha256.Sum256([]byte(uri))
	return hex.EncodeToString(sum[:])
}
