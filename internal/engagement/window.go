// Package engagement keeps rolling like/repost counts per target post and
// detects when a target crosses the alert threshold. It's pure logic: time is
// the events' own time_us, never the clock, so it's exact for a worker that is
// behind, deterministic after a replay, and simple to test.
package engagement

import (
	"slices"
	"time"
)

type Kind int

const (
	Like Kind = iota
	Repost
)

// Crossing is returned when a target reaches the threshold.
type Crossing struct {
	Target         string
	Likes, Reposts int
	AtUS           int64 // event time of the like/repost that crossed it
}

type entry struct {
	timeUS int64
	kind   Kind
}

type target struct {
	entries []entry // sorted by time
	alerted bool    // alerted for the current burst
}

// Window counts likes + reposts per target over the last Size of event time.
// Not safe for concurrent use.
type Window struct {
	Size      time.Duration
	Threshold int

	targets   map[string]*target
	seen      map[string]int64 // like/repost URI -> time, to drop duplicates
	nowUS     int64            // newest event time seen
	lastSweep int64
}

func New(size time.Duration, threshold int) *Window {
	return &Window{Size: size, Threshold: threshold, targets: map[string]*target{}, seen: map[string]int64{}}
}

// Add counts one like or repost. It returns a Crossing the first time the
// target reaches the threshold within its current burst; a target alerts again
// only after it has dropped out of the window entirely. Duplicates (same uri)
// and events older than the window are ignored.
func (w *Window) Add(uri, targetURI string, kind Kind, timeUS int64) (Crossing, bool) {
	if _, dup := w.seen[uri]; dup {
		return Crossing{}, false
	}
	if timeUS > w.nowUS {
		w.nowUS = timeUS
	}
	cutoff := w.nowUS - w.Size.Microseconds()
	if timeUS <= cutoff {
		return Crossing{}, false // already outside the window
	}
	w.seen[uri] = timeUS

	t := w.targets[targetURI]
	if t == nil {
		t = &target{}
		w.targets[targetURI] = t
	}
	// Events arrive in time order almost always; a replay overlap or a
	// redelivery can bring an older one, so insert in place.
	i, _ := slices.BinarySearchFunc(t.entries, timeUS, func(e entry, v int64) int { return cmp(e.timeUS, v) })
	t.entries = slices.Insert(t.entries, i, entry{timeUS, kind})
	t.expire(cutoff)

	w.sweep(cutoff)

	if t.alerted || len(t.entries) < w.Threshold {
		return Crossing{}, false
	}
	t.alerted = true
	likes, reposts := t.counts()
	return Crossing{Target: targetURI, Likes: likes, Reposts: reposts, AtUS: timeUS}, true
}

// Count returns the target's current likes and reposts in the window.
func (w *Window) Count(targetURI string) (likes, reposts int) {
	t := w.targets[targetURI]
	if t == nil {
		return 0, 0
	}
	t.expire(w.nowUS - w.Size.Microseconds())
	return t.counts()
}

// Targets is how many targets are currently tracked (for stats).
func (w *Window) Targets() int { return len(w.targets) }

func (t *target) expire(cutoff int64) {
	n := 0
	for n < len(t.entries) && t.entries[n].timeUS <= cutoff {
		n++
	}
	t.entries = t.entries[n:]
}

func (t *target) counts() (likes, reposts int) {
	for _, e := range t.entries {
		if e.kind == Like {
			likes++
		} else {
			reposts++
		}
	}
	return likes, reposts
}

// sweep drops targets and seen URIs that left the window, at most once per
// window of event time, so memory follows the window, not the stream.
func (w *Window) sweep(cutoff int64) {
	if w.nowUS-w.lastSweep < w.Size.Microseconds() {
		return
	}
	w.lastSweep = w.nowUS
	for k, t := range w.targets {
		if t.expire(cutoff); len(t.entries) == 0 {
			delete(w.targets, k) // a later burst alerts again
		}
	}
	for k, ts := range w.seen {
		if ts <= cutoff {
			delete(w.seen, k)
		}
	}
}

func cmp(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
