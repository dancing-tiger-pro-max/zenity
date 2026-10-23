// Package monitor watches each path's stream and consumer, warns before a slow
// path starts losing unread messages, and reports exactly how many it lost.
// It reads only state NATS already keeps, and runs as its own pod so neither
// a monitoring bug nor an ingester outage can affect the other.
package monitor

import "time"

// Snapshot is what one check sees for one path.
type Snapshot struct {
	Msgs, Bytes        uint64    // stored in the stream now
	FirstSeq           uint64    // oldest message still stored
	FirstTime          time.Time // when it was stored
	MaxBytes           int64
	MaxAge             time.Duration
	Pending            uint64 // not yet delivered to the consumer
	DeliveredSeq       uint64 // last stream sequence handed to the consumer
	Deliveries         uint64 // consumer sequence: how many deliveries so far
	Now                time.Time
	FullRatio, Backlog float64 // thresholds from config
}

// pathState is what the monitor remembers per path between checks.
type pathState struct {
	warning  bool
	lostUpTo uint64 // highest stream sequence already counted as lost
	seen     bool   // prev* hold the previous check
	prevSeq  uint64 // DeliveredSeq at the previous check
	prevDlv  uint64 // Deliveries at the previous check
}

// Assessment is what changed in one check.
type Assessment struct {
	NearFull     bool
	Unread       float64 // share of stored messages not yet delivered
	StartWarning bool    // backlog just became dangerous
	EndWarning   bool    // and just stopped being
	LostFrom     uint64  // newly lost range, if LostCount > 0
	LostTo       uint64
	LostCount    uint64
}

// assess compares one snapshot with what was seen before. Loss: messages are
// evicted from the front of the stream; any evicted sequence the consumer was
// never handed is lost. Each lost range is reported once.
func assess(s Snapshot, st *pathState) Assessment {
	var a Assessment
	if s.Msgs > 0 {
		a.Unread = float64(s.Pending) / float64(s.Msgs)
	}
	bytesFull := s.MaxBytes > 0 && float64(s.Bytes) >= s.FullRatio*float64(s.MaxBytes)
	ageFull := s.MaxAge > 0 && !s.FirstTime.IsZero() && s.Now.Sub(s.FirstTime) >= time.Duration(s.FullRatio*float64(s.MaxAge))
	a.NearFull = bytesFull || ageFull

	danger := a.NearFull && a.Unread >= s.Backlog
	a.StartWarning = danger && !st.warning
	a.EndWarning = !danger && st.warning
	st.warning = danger

	// Messages are evicted from the front of the stream. Two ways one is lost:
	//
	// 1. Skipped: when the front passes the consumer's position, its next
	//    delivery jumps to the new front. Between checks its position moved
	//    by more than the number of deliveries it received; the difference was
	//    jumped over. (A redelivery also counts as a delivery, so this can only
	//    undercount, never invent loss.)
	// 2. Ahead: evicted messages past the consumer's position right now.
	//
	// Each lost sequence is counted once.
	if st.seen && s.Deliveries >= st.prevDlv {
		base := max(st.prevSeq, st.lostUpTo)
		if s.DeliveredSeq > base {
			moved, got := s.DeliveredSeq-base, s.Deliveries-st.prevDlv
			if moved > got {
				a.LostCount += moved - got
				a.LostFrom, a.LostTo = base+1, s.DeliveredSeq
			}
		}
	}
	st.seen, st.prevSeq, st.prevDlv = true, s.DeliveredSeq, s.Deliveries

	if s.FirstSeq > 0 {
		evictedTo := s.FirstSeq - 1
		from := max(s.DeliveredSeq, st.lostUpTo) + 1
		if evictedTo >= from {
			a.LostCount += evictedTo - from + 1
			if a.LostFrom == 0 {
				a.LostFrom = from
			}
			a.LostTo = evictedTo
			st.lostUpTo = evictedTo
		}
	}
	return a
}
