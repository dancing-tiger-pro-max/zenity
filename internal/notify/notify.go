// Package notify delivers alerts. Workers and the monitor depend only on the
// Notifier interface; today's sink is a log line, and a webhook or Slack sink
// would be another implementation.
package notify

import (
	"context"
	"log/slog"
)

// Alert is one notification. ID is deterministic, so a receiver can drop the
// repeats that at-least-once delivery can cause. Attrs carry metadata only,
// never post text.
type Alert struct {
	ID    string
	Type  string
	Attrs []any // key, value pairs
}

type Notifier interface {
	Notify(ctx context.Context, a Alert) error
}

// Log writes each alert as one JSON log line.
type Log struct {
	Logger *slog.Logger
}

func (l Log) Notify(_ context.Context, a Alert) error {
	l.Logger.Info("ALERT", append([]any{"alert_id", a.ID, "alert_type", a.Type}, a.Attrs...)...)
	return nil
}
