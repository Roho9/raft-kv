// Package kv holds the replicated key-value state machine.
package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"

	pb "github.com/Roho9/raft-kv/gen/raftkvpb"
)

// Result is the outcome of applying one command.
type Result struct {
	Value   []byte
	Found   bool
	Existed bool
}

// session tracks the last operation applied for one client so retried
// requests (same client id and sequence number) are not applied twice.
type session struct {
	Seq    uint64
	Result Result
}

// StateMachine is a deterministic map driven by committed Raft entries.
// It is only touched from the server's single apply loop (including
// Snapshot and Restore), so it needs no internal locking.
type StateMachine struct {
	data     map[string][]byte
	sessions map[string]session
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		data:     make(map[string][]byte),
		sessions: make(map[string]session),
	}
}

// Apply executes a committed command exactly once. A command whose sequence
// number was already applied for its client returns the cached result, which
// makes client retries idempotent.
func (sm *StateMachine) Apply(cmd *pb.Command) Result {
	if cmd.ClientId != "" {
		if sess, ok := sm.sessions[cmd.ClientId]; ok && cmd.Seq <= sess.Seq {
			return sess.Result
		}
	}

	var res Result
	switch cmd.Op {
	case pb.Command_GET:
		v, ok := sm.data[cmd.Key]
		res = Result{Value: v, Found: ok}
	case pb.Command_PUT:
		sm.data[cmd.Key] = cmd.Value
		res = Result{}
	case pb.Command_DELETE:
		_, existed := sm.data[cmd.Key]
		delete(sm.data, cmd.Key)
		res = Result{Existed: existed}
	}

	if cmd.ClientId != "" {
		sm.sessions[cmd.ClientId] = session{Seq: cmd.Seq, Result: res}
	}
	return res
}

// Len returns the number of keys stored.
func (sm *StateMachine) Len() int {
	return len(sm.data)
}

type snapshotState struct {
	Data     map[string][]byte
	Sessions map[string]session
}

// Snapshot serializes the full state, including client sessions so
// idempotency survives log compaction.
func (sm *StateMachine) Snapshot() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(snapshotState{Data: sm.data, Sessions: sm.sessions}); err != nil {
		panic(fmt.Sprintf("kv: encode snapshot: %v", err))
	}
	return buf.Bytes()
}

// Restore replaces the state with a snapshot produced by Snapshot.
func (sm *StateMachine) Restore(data []byte) {
	var st snapshotState
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&st); err != nil {
		panic(fmt.Sprintf("kv: decode snapshot: %v", err))
	}
	if st.Data == nil {
		st.Data = make(map[string][]byte)
	}
	if st.Sessions == nil {
		st.Sessions = make(map[string]session)
	}
	sm.data = st.Data
	sm.sessions = st.Sessions
}
