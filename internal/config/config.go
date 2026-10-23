// Package config loads and validates config.yaml, the single source of settings.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Jetstream Jetstream `yaml:"jetstream"`
	NATS      NATS      `yaml:"nats"`
	Streams   Streams   `yaml:"streams"`
	Workers   Workers   `yaml:"workers"`
	Monitor   Monitor   `yaml:"monitor"`
}

type Jetstream struct {
	URL              string          `yaml:"url"`
	CursorInterval   time.Duration   `yaml:"cursor_interval"`   // how often the cursor is saved
	MaxPending       int             `yaml:"max_pending"`       // unconfirmed publishes in flight
	PublishTimeout   time.Duration   `yaml:"publish_timeout"`   // an unconfirmed publish after this is an error
	ReconnectBackoff []time.Duration `yaml:"reconnect_backoff"` // waits between reconnect attempts (last repeats)
	GapThreshold     time.Duration   `yaml:"gap_threshold"`     // a resume that jumps further than this is a gap
}

type NATS struct {
	URL string `yaml:"url"`
}

type Streams struct {
	DuplicatesWindow time.Duration `yaml:"duplicates_window"` // dedupe by message ID
	Posts            StreamLimits  `yaml:"posts"`
	Likes            StreamLimits  `yaml:"likes"`
	Deletes          StreamLimits  `yaml:"deletes"`
	DLQ              StreamLimits  `yaml:"dlq"`
}

type StreamLimits struct {
	MaxAge   time.Duration `yaml:"max_age"`
	MaxBytes ByteSize      `yaml:"max_bytes"`
}

type Workers struct {
	AckWait    time.Duration   `yaml:"ack_wait"`
	MaxDeliver int             `yaml:"max_deliver"`
	Backoff    []time.Duration `yaml:"backoff"` // delay before each retry
	Posts      PostsWorker     `yaml:"posts"`
	Likes      LikesWorker     `yaml:"likes"`
	Deletes    DeletesWorker   `yaml:"deletes"`
}

type PostsWorker struct {
	ProcessDelay time.Duration `yaml:"process_delay"`
	Keywords     []string      `yaml:"keywords"`  // case-insensitive substring match on the text
	Languages    []string      `yaml:"languages"` // match against the post's langs
}

type LikesWorker struct {
	ProcessDelay time.Duration `yaml:"process_delay"`
	Window       time.Duration `yaml:"window"`    // rolling window, in event time
	Threshold    int           `yaml:"threshold"` // likes + reposts on one target within the window
}

type DeletesWorker struct {
	ProcessDelay time.Duration `yaml:"process_delay"`
	TombstoneTTL time.Duration `yaml:"tombstone_ttl"`
}

type Monitor struct {
	Interval     time.Duration `yaml:"interval"`
	FullRatio    float64       `yaml:"full_ratio"`    // stream counts as near its limit at this share of max_bytes/max_age
	BacklogRatio float64       `yaml:"backlog_ratio"` // warn when this share of a near-full stream is unprocessed
}

// Load reads and validates the file at path. Every problem is reported at once,
// and invalid config fails loudly instead of running with surprising behavior.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true) // a typo in a key is an error, not a silently ignored setting
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := c.validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config %s:\n%w", path, err)
	}
	c.normalize()
	return c, nil
}

func (c Config) validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf("  - "+format, args...))
		}
	}

	check(c.Jetstream.URL != "", "jetstream.url is required")
	check(c.Jetstream.CursorInterval > 0, "jetstream.cursor_interval must be > 0")
	check(c.Jetstream.MaxPending > 0, "jetstream.max_pending must be > 0")
	check(c.Jetstream.PublishTimeout > 0, "jetstream.publish_timeout must be > 0")
	check(len(c.Jetstream.ReconnectBackoff) > 0, "jetstream.reconnect_backoff needs at least one delay")
	check(c.Jetstream.GapThreshold > 0, "jetstream.gap_threshold must be > 0")
	check(c.NATS.URL != "", "nats.url is required")

	check(c.Streams.DuplicatesWindow > c.Jetstream.CursorInterval,
		"streams.duplicates_window (%s) must be longer than jetstream.cursor_interval (%s), or replays after a restart get past dedupe",
		c.Streams.DuplicatesWindow, c.Jetstream.CursorInterval)
	for name, s := range map[string]StreamLimits{"posts": c.Streams.Posts, "likes": c.Streams.Likes, "deletes": c.Streams.Deletes, "dlq": c.Streams.DLQ} {
		check(s.MaxAge > 0, "streams.%s.max_age must be > 0", name)
		check(s.MaxBytes > 0, "streams.%s.max_bytes must be > 0", name)
	}
	check(c.Streams.Likes.MaxAge >= c.Workers.Likes.Window,
		"streams.likes.max_age (%s) must cover workers.likes.window (%s), or the replay after a restart has nothing to read",
		c.Streams.Likes.MaxAge, c.Workers.Likes.Window)

	w := c.Workers
	check(w.AckWait > 0, "workers.ack_wait must be > 0")
	check(w.MaxDeliver > 1, "workers.max_deliver must be > 1")
	check(len(w.Backoff) > 0, "workers.backoff needs at least one delay")
	for name, d := range map[string]time.Duration{"posts": w.Posts.ProcessDelay, "likes": w.Likes.ProcessDelay, "deletes": w.Deletes.ProcessDelay} {
		check(d >= 0, "workers.%s.process_delay must be >= 0", name)
		check(d < w.AckWait, "workers.%s.process_delay (%s) must be shorter than workers.ack_wait (%s), or NATS redelivers messages still being handled",
			name, d, w.AckWait)
	}
	check(len(w.Posts.Keywords)+len(w.Posts.Languages) > 0,
		"workers.posts needs at least one keyword or language, or it never matches anything")
	for _, k := range w.Posts.Keywords {
		check(strings.TrimSpace(k) != "", "workers.posts.keywords contains an empty keyword")
	}
	check(w.Likes.Window > 0, "workers.likes.window must be > 0")
	check(w.AckWait < w.Likes.Window,
		"workers.ack_wait (%s) must be shorter than workers.likes.window (%s): likes redelivered after a crash would arrive too late to count",
		w.AckWait, w.Likes.Window)
	check(w.Likes.Threshold > 0, "workers.likes.threshold must be > 0")
	check(w.Deletes.TombstoneTTL >= c.Streams.Posts.MaxAge,
		"workers.deletes.tombstone_ttl (%s) must be >= streams.posts.max_age (%s): the posts worker can lag up to its full buffer",
		w.Deletes.TombstoneTTL, c.Streams.Posts.MaxAge)

	check(c.Monitor.Interval > 0, "monitor.interval must be > 0")
	check(c.Monitor.FullRatio > 0 && c.Monitor.FullRatio <= 1, "monitor.full_ratio must be in (0, 1]")
	check(c.Monitor.BacklogRatio > 0 && c.Monitor.BacklogRatio <= 1, "monitor.backlog_ratio must be in (0, 1]")

	return errors.Join(errs...)
}

// normalize lowercases match lists so matching is case-insensitive.
func (c *Config) normalize() {
	for i, k := range c.Workers.Posts.Keywords {
		c.Workers.Posts.Keywords[i] = strings.ToLower(strings.TrimSpace(k))
	}
	for i, l := range c.Workers.Posts.Languages {
		c.Workers.Posts.Languages[i] = strings.ToLower(strings.TrimSpace(l))
	}
}
