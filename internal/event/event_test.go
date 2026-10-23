package event

import "testing"

// Captured from the live feed: one commit that creates two posts of a thread.
// Both events share did and time_us, so a did:time_us dedupe ID would make NATS
// drop the second post as a "duplicate".
const (
	threadPost1 = `{"did":"did:plc:a","time_us":1792760000000001,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"3mykivaxnuk2n","rev":"3mykivb7efv2s"}}`
	threadPost2 = `{"did":"did:plc:a","time_us":1792760000000001,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"3mykivb5fhk2n","rev":"3mykivb7efv2s"}}`
)

func TestMsgIDUniquePerRecordInOneCommit(t *testing.T) {
	a, err := Parse([]byte(threadPost1))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Parse([]byte(threadPost2))
	if err != nil {
		t.Fatal(err)
	}
	if a.MsgID() == b.MsgID() {
		t.Fatalf("two records from one commit got the same ID %q", a.MsgID())
	}
	// The same event resent (e.g. after a reconnect) must keep its ID.
	again, _ := Parse([]byte(threadPost1))
	if again.MsgID() != a.MsgID() {
		t.Fatalf("a resent event got a different ID: %q vs %q", again.MsgID(), a.MsgID())
	}
}
