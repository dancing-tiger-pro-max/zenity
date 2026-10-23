package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

// CursorStore keeps the ingester's resume point in NATS KV, so it fails
// together with the broker: if NATS loses its data, it loses the cursor too.
type CursorStore struct {
	kv  jetstream.KeyValue
	key string
}

func NewCursorStore(ctx context.Context, js jetstream.JetStream, key string) (*CursorStore, error) {
	kv, err := js.KeyValue(ctx, BucketCursor)
	if err != nil {
		return nil, fmt.Errorf("cursor bucket: %w", err)
	}
	return &CursorStore{kv: kv, key: key}, nil
}

// Load returns the saved cursor, or ok=false if none was ever saved.
func (s *CursorStore) Load(ctx context.Context) (timeUS int64, ok bool, err error) {
	e, err := s.kv.Get(ctx, s.key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("load cursor: %w", err)
	}
	v, err := strconv.ParseInt(string(e.Value()), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("cursor value %q: %w", e.Value(), err)
	}
	return v, true, nil
}

func (s *CursorStore) Save(ctx context.Context, timeUS int64) error {
	if _, err := s.kv.Put(ctx, s.key, []byte(strconv.FormatInt(timeUS, 10))); err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}
	return nil
}
