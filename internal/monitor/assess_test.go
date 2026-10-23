package monitor

import (
	"testing"
	"time"
)

func snap(msgs, bytes, firstSeq, pending, delivered uint64) Snapshot {
	return snapD(msgs, bytes, firstSeq, pending, delivered, delivered)
}

// snapD also sets the consumer's delivery count (it differs from the
// delivered stream sequence once the consumer has jumped over evicted messages).
func snapD(msgs, bytes, firstSeq, pending, delivered, deliveries uint64) Snapshot {
	now := time.Unix(1_000_000, 0)
	return Snapshot{
		Msgs: msgs, Bytes: bytes, FirstSeq: firstSeq, FirstTime: now.Add(-time.Minute),
		MaxBytes: 1000, MaxAge: 24 * time.Hour,
		Pending: pending, DeliveredSeq: delivered, Deliveries: deliveries, Now: now,
		FullRatio: 0.9, Backlog: 0.8,
	}
}

func TestBacklogWarning(t *testing.T) {
	st := &pathState{}
	// A full stream that's all processed is healthy: limits retention keeps
	// processed messages, so "full" alone means nothing.
	if a := assess(snap(100, 950, 1, 0, 100), st); a.StartWarning {
		t.Fatal("warned on a full but fully processed stream")
	}
	// A big backlog in a stream with room is safe: nothing gets evicted yet.
	if a := assess(snap(100, 500, 1, 90, 10), st); a.StartWarning {
		t.Fatal("warned on a backlog in a stream that isn't near its limit")
	}
	// Near full and mostly unread: eviction is about to hit unread messages.
	a := assess(snap(100, 950, 1, 85, 15), st)
	if !a.StartWarning {
		t.Fatalf("no warning at %.0f%% unread in a full stream", a.Unread*100)
	}
	// Still dangerous: warn once, not on every check.
	if a := assess(snap(100, 950, 1, 86, 15), st); a.StartWarning {
		t.Fatal("warned again while already warning")
	}
	// Worker caught up: the warning ends.
	if a := assess(snap(100, 950, 1, 5, 95), st); !a.EndWarning {
		t.Fatal("warning didn't end after the backlog cleared")
	}
}

func TestLossIsReportedOncePerRange(t *testing.T) {
	st := &pathState{}
	// Consumer handed messages up to 40; the stream still starts at 31: nothing lost.
	if a := assess(snap(100, 950, 31, 90, 40), st); a.LostCount != 0 {
		t.Fatalf("reported %d lost with nothing evicted past the consumer", a.LostCount)
	}
	// Eviction moved the first message to 61: 41..60 were never handed out.
	a := assess(snap(100, 950, 61, 90, 40), st)
	if a.LostCount != 20 || a.LostFrom != 41 || a.LostTo != 60 {
		t.Fatalf("lost %d (%d..%d), want 20 (41..60)", a.LostCount, a.LostFrom, a.LostTo)
	}
	// Same state on the next check: the same range isn't reported twice.
	if a := assess(snap(100, 950, 61, 90, 40), st); a.LostCount != 0 {
		t.Fatalf("re-reported %d lost", a.LostCount)
	}
	// More eviction: only the new part.
	if a := assess(snap(100, 950, 71, 90, 40), st); a.LostCount != 10 || a.LostFrom != 61 {
		t.Fatalf("lost %d from %d, want 10 from 61", a.LostCount, a.LostFrom)
	}
}

// The case the first version missed, found in the live demo: a slow consumer
// whose next delivery jumps over evicted messages. Its position moves far more
// than the number of messages it actually received.
func TestLossWhenConsumerJumpsOverEvictedMessages(t *testing.T) {
	st := &pathState{}
	// Consumer at stream seq 100, 100 deliveries so far; stream starts at 50.
	assess(snapD(1000, 950, 50, 900, 100, 100), st)
	// Five seconds later: the front moved to 2001, the consumer received only
	// 20 more messages but its position is now 2020 (it jumped to the front).
	a := assess(snapD(1000, 950, 2001, 900, 2020, 120), st)
	// It passed 1,920 sequences (101..2020) and received 20: 1,900 were lost.
	if a.LostCount != 1900 {
		t.Fatalf("lost %d, want 1900", a.LostCount)
	}
	// Next check, keeping up (moved 10, received 10): nothing more lost.
	if a := assess(snapD(1000, 950, 2005, 0, 2030, 130), st); a.LostCount != 0 {
		t.Fatalf("lost %d while keeping up", a.LostCount)
	}
}
