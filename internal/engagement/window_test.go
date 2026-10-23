package engagement

import (
	"fmt"
	"testing"
	"time"
)

const sec = int64(time.Second / time.Microsecond)

func TestRollingWindowAndThreshold(t *testing.T) {
	w := New(time.Minute, 3)
	t0 := int64(1_000_000) * sec

	// Two likes and a repost on post A within the minute: the third crosses.
	if _, ok := w.Add("like1", "A", Like, t0); ok {
		t.Fatal("crossed at 1")
	}
	if _, ok := w.Add("like2", "A", Like, t0+10*sec); ok {
		t.Fatal("crossed at 2")
	}
	c, ok := w.Add("repost1", "A", Repost, t0+20*sec)
	if !ok || c.Likes != 2 || c.Reposts != 1 || c.AtUS != t0+20*sec {
		t.Fatalf("third event: got %+v, %v; want a crossing with 2 likes, 1 repost", c, ok)
	}

	// More engagement in the same burst doesn't alert again.
	if _, ok := w.Add("like3", "A", Like, t0+30*sec); ok {
		t.Fatal("alerted twice in one burst")
	}

	// Rolling: 65s after the first like, it has left the window.
	w.Add("other", "B", Like, t0+65*sec)
	if likes, reposts := w.Count("A"); likes != 2 || reposts != 1 {
		t.Fatalf("after expiry: %d likes, %d reposts; want 2 and 1", likes, reposts)
	}
}

func TestDuplicatesAreCountedOnce(t *testing.T) {
	w := New(time.Minute, 2)
	t0 := int64(1_000_000) * sec
	w.Add("like1", "A", Like, t0)
	// The same like delivered again (redelivery, or replay overlap).
	if _, ok := w.Add("like1", "A", Like, t0); ok {
		t.Fatal("a duplicate crossed the threshold")
	}
	if likes, _ := w.Count("A"); likes != 1 {
		t.Fatalf("likes = %d, want 1", likes)
	}
}

// The window is event time: a worker hours behind counts exactly the same as
// one that's live, and a burst spread wider than the window never crosses.
func TestEventTimeNotClock(t *testing.T) {
	w := New(time.Minute, 3)
	old := int64(1_000_000) * sec // long before "now" on any clock
	w.Add("l1", "A", Like, old)
	w.Add("l2", "A", Like, old+40*sec)
	if _, ok := w.Add("l3", "A", Like, old+80*sec); ok {
		t.Fatal("likes 80s apart crossed a 1-minute window")
	}
	if _, ok := w.Add("l4", "A", Like, old+90*sec); !ok {
		t.Fatal("3 likes within 50s didn't cross")
	}
}

// A target alerts again only after its burst has fully left the window.
func TestNewBurstAlertsAgain(t *testing.T) {
	w := New(time.Minute, 2)
	t0 := int64(1_000_000) * sec
	w.Add("a1", "A", Like, t0)
	if _, ok := w.Add("a2", "A", Like, t0+sec); !ok {
		t.Fatal("first burst didn't cross")
	}
	// Other traffic moves event time on (minutes 5-7), sweeping A out.
	for i := 0; i < 3; i++ {
		w.Add(fmt.Sprint("x", i), "X", Like, t0+int64(5+i)*60*sec)
	}
	if w.Count("A"); w.targets["A"] != nil {
		t.Fatal("expired target still tracked: memory would grow with the stream")
	}
	later := t0 + 10*60*sec
	w.Add("a3", "A", Like, later)
	if _, ok := w.Add("a4", "A", Like, later+sec); !ok {
		t.Fatal("a new burst after the old one expired didn't alert")
	}
}
