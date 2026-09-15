package kv

import (
	"testing"

	pb "github.com/Roho9/raft-kv/gen/raftkvpb"
)

func TestApplyPutGetDelete(t *testing.T) {
	sm := NewStateMachine()

	sm.Apply(&pb.Command{Op: pb.Command_PUT, Key: "a", Value: []byte("1"), ClientId: "c1", Seq: 1})
	res := sm.Apply(&pb.Command{Op: pb.Command_GET, Key: "a", ClientId: "c1", Seq: 2})
	if !res.Found || string(res.Value) != "1" {
		t.Fatalf("get a = %+v, want found=true value=1", res)
	}

	res = sm.Apply(&pb.Command{Op: pb.Command_DELETE, Key: "a", ClientId: "c1", Seq: 3})
	if !res.Existed {
		t.Fatalf("delete a: existed=false, want true")
	}

	res = sm.Apply(&pb.Command{Op: pb.Command_GET, Key: "a", ClientId: "c1", Seq: 4})
	if res.Found {
		t.Fatalf("get a after delete: found=true, want false")
	}

	if sm.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", sm.Len())
	}
}

// TestApplyIsIdempotent verifies the core exactly-once guarantee the
// replicated client relies on: replaying a (client, seq) pair that was
// already applied returns the cached result instead of re-executing.
func TestApplyIsIdempotent(t *testing.T) {
	sm := NewStateMachine()

	sm.Apply(&pb.Command{Op: pb.Command_PUT, Key: "counter", Value: []byte("first"), ClientId: "c1", Seq: 5})
	sm.Apply(&pb.Command{Op: pb.Command_PUT, Key: "counter", Value: []byte("second"), ClientId: "c1", Seq: 6})

	// Replaying seq 5 (e.g. a retried RPC that actually landed earlier)
	// must not overwrite the value written by seq 6.
	sm.Apply(&pb.Command{Op: pb.Command_PUT, Key: "counter", Value: []byte("replayed"), ClientId: "c1", Seq: 5})

	res := sm.Apply(&pb.Command{Op: pb.Command_GET, Key: "counter", ClientId: "c1", Seq: 7})
	if string(res.Value) != "second" {
		t.Fatalf("counter = %q, want %q (a replayed old seq overwrote a newer write)", res.Value, "second")
	}
}

// TestApplyIdempotentReturnsCachedResult checks that a retried delete
// returns the same Existed value it returned the first time, not a fresh
// (and wrong) computation against the now-mutated map.
func TestApplyIdempotentReturnsCachedResult(t *testing.T) {
	sm := NewStateMachine()
	sm.Apply(&pb.Command{Op: pb.Command_PUT, Key: "k", Value: []byte("v"), ClientId: "c1", Seq: 1})

	first := sm.Apply(&pb.Command{Op: pb.Command_DELETE, Key: "k", ClientId: "c1", Seq: 2})
	if !first.Existed {
		t.Fatalf("first delete: existed=false, want true")
	}
	// Retry the exact same operation (same client, same seq). The key is
	// already gone, so a naive re-execution would report existed=false.
	replay := sm.Apply(&pb.Command{Op: pb.Command_DELETE, Key: "k", ClientId: "c1", Seq: 2})
	if !replay.Existed {
		t.Fatalf("replayed delete: existed=%v, want true (cached first result)", replay.Existed)
	}
}

// TestDifferentClientsAreIndependent ensures the session table is keyed by
// client id, not just sequence number.
func TestDifferentClientsAreIndependent(t *testing.T) {
	sm := NewStateMachine()
	sm.Apply(&pb.Command{Op: pb.Command_PUT, Key: "shared", Value: []byte("from-c1"), ClientId: "c1", Seq: 1})
	res := sm.Apply(&pb.Command{Op: pb.Command_PUT, Key: "shared", Value: []byte("from-c2"), ClientId: "c2", Seq: 1})
	if res.Existed {
		// PUT doesn't set Existed, this just confirms it ran (not deduped
		// against client c1's seq 1).
	}
	got := sm.Apply(&pb.Command{Op: pb.Command_GET, Key: "shared", ClientId: "c1", Seq: 2})
	if string(got.Value) != "from-c2" {
		t.Fatalf("shared = %q, want %q (client c2's write should not be deduped against c1's session)", got.Value, "from-c2")
	}
}

// TestSnapshotRoundTrip verifies both the key/value data and the client
// session table (needed for idempotency) survive a snapshot and restore.
func TestSnapshotRoundTrip(t *testing.T) {
	sm := NewStateMachine()
	sm.Apply(&pb.Command{Op: pb.Command_PUT, Key: "a", Value: []byte("1"), ClientId: "c1", Seq: 1})
	sm.Apply(&pb.Command{Op: pb.Command_PUT, Key: "b", Value: []byte("2"), ClientId: "c1", Seq: 2})

	snap := sm.Snapshot()

	restored := NewStateMachine()
	restored.Restore(snap)

	if restored.Len() != 2 {
		t.Fatalf("restored Len() = %d, want 2", restored.Len())
	}
	res := restored.Apply(&pb.Command{Op: pb.Command_GET, Key: "a", ClientId: "c1", Seq: 3})
	if !res.Found || string(res.Value) != "1" {
		t.Fatalf("restored get a = %+v, want found=true value=1", res)
	}

	// The session table must have survived too: replaying seq 1 (already
	// applied before the snapshot was taken) must not re-execute.
	replay := restored.Apply(&pb.Command{Op: pb.Command_PUT, Key: "a", Value: []byte("clobbered"), ClientId: "c1", Seq: 1})
	_ = replay
	final := restored.Apply(&pb.Command{Op: pb.Command_GET, Key: "a", ClientId: "c1", Seq: 4})
	if string(final.Value) != "1" {
		t.Fatalf("after replaying a pre-snapshot seq, a = %q, want unchanged %q", final.Value, "1")
	}
}

func TestRestoreOfEmptySnapshot(t *testing.T) {
	sm := NewStateMachine()
	sm.Restore(NewStateMachine().Snapshot())
	if sm.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", sm.Len())
	}
	res := sm.Apply(&pb.Command{Op: pb.Command_GET, Key: "missing", ClientId: "c1", Seq: 1})
	if res.Found {
		t.Fatalf("get on empty restored state machine: found=true, want false")
	}
}
