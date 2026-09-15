package raft

import (
	"os"
	"path/filepath"
	"testing"

	pb "github.com/Roho9/raft-kv/gen/raftkvpb"
)

func openTestStorage(t *testing.T, dir string) *Storage {
	t.Helper()
	s, err := OpenStorage(dir, false)
	if err != nil {
		t.Fatalf("OpenStorage: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func entry(index, term uint64, data string) *pb.LogEntry {
	return &pb.LogEntry{Index: index, Term: term, Data: []byte(data)}
}

func TestFreshStorageIsEmpty(t *testing.T) {
	s := openTestStorage(t, t.TempDir())
	if got := s.Meta(); got.Term != 0 || got.VotedFor != 0 {
		t.Fatalf("Meta() = %+v, want zero value", got)
	}
	if len(s.Entries()) != 0 {
		t.Fatalf("Entries() = %v, want empty", s.Entries())
	}
	idx, term := s.SnapshotMeta()
	if idx != 0 || term != 0 {
		t.Fatalf("SnapshotMeta() = (%d, %d), want (0, 0)", idx, term)
	}
}

func TestMetaPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s := openTestStorage(t, dir)
	s.SaveMeta(7, 3)
	s.Close()

	s2 := openTestStorage(t, dir)
	got := s2.Meta()
	if got.Term != 7 || got.VotedFor != 3 {
		t.Fatalf("Meta() after reopen = %+v, want {Term:7 VotedFor:3}", got)
	}
}

func TestAppendEntriesPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s := openTestStorage(t, dir)
	entries := []*pb.LogEntry{entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c")}
	s.AppendEntries(entries)
	s.Close()

	s2 := openTestStorage(t, dir)
	got := s2.Entries()
	if len(got) != len(entries) {
		t.Fatalf("Entries() has %d entries, want %d", len(got), len(entries))
	}
	for i, e := range entries {
		if got[i].Index != e.Index || got[i].Term != e.Term || string(got[i].Data) != string(e.Data) {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], e)
		}
	}
}

// TestAppendNoSyncThenSync exercises the group-commit path: entries written
// without an immediate fsync must still be recovered after a clean close,
// as long as Sync was called before Close.
func TestAppendNoSyncThenSync(t *testing.T) {
	dir := t.TempDir()
	s := openTestStorage(t, dir)
	s.AppendNoSync([]*pb.LogEntry{entry(1, 1, "a")})
	s.AppendNoSync([]*pb.LogEntry{entry(2, 1, "b")})
	if len(s.Entries()) != 2 {
		t.Fatalf("Entries() before Sync = %d, want 2 (in-memory append happens immediately)", len(s.Entries()))
	}
	s.Sync()
	s.Close()

	s2 := openTestStorage(t, dir)
	if len(s2.Entries()) != 2 {
		t.Fatalf("Entries() after reopen = %d, want 2", len(s2.Entries()))
	}
}

// TestTornTailIsTruncated simulates a crash mid-write: a record whose
// header or payload is incomplete on disk. Recovery must keep every
// complete record before it and discard the torn bytes, since an
// unacknowledged tail write is the only thing a crash can produce here.
func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	s := openTestStorage(t, dir)
	s.AppendEntries([]*pb.LogEntry{entry(1, 1, "a"), entry(2, 1, "b")})
	s.Close()

	walPath := filepath.Join(dir, walFile)
	full, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	// Append a truncated record: a well-formed 8-byte header claiming a
	// large payload, followed by only a few garbage bytes.
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{100, 0, 0, 0, 0xAB, 0xCD, 0xEF, 0x01, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2 := openTestStorage(t, dir)
	got := s2.Entries()
	if len(got) != 2 {
		t.Fatalf("Entries() after torn tail = %d, want 2 (valid prefix only)", len(got))
	}

	// Recovery must also truncate the file on disk, or the torn bytes
	// would poison every future recovery.
	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != full.Size() {
		t.Fatalf("wal size after recovery = %d, want %d (torn tail not truncated)", after.Size(), full.Size())
	}
}

func TestRewriteLogReplacesContents(t *testing.T) {
	dir := t.TempDir()
	s := openTestStorage(t, dir)
	s.AppendEntries([]*pb.LogEntry{entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")})

	// Simulate a follower truncating a conflicting suffix and keeping only
	// entry 1.
	s.RewriteLog([]*pb.LogEntry{entry(1, 1, "a")})
	if len(s.Entries()) != 1 {
		t.Fatalf("Entries() after RewriteLog = %d, want 1", len(s.Entries()))
	}

	// AppendEntries must still work against the reopened file handle.
	s.AppendEntries([]*pb.LogEntry{entry(2, 2, "new-b")})
	s.Close()

	s2 := openTestStorage(t, dir)
	got := s2.Entries()
	if len(got) != 2 || got[1].Term != 2 || string(got[1].Data) != "new-b" {
		t.Fatalf("Entries() after reopen = %+v, want [index1 term1, index2 term2 new-b]", got)
	}
}

func TestSaveSnapshotPersistsAndCompactsLog(t *testing.T) {
	dir := t.TempDir()
	s := openTestStorage(t, dir)
	entries := []*pb.LogEntry{entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c"), entry(4, 2, "d")}
	s.AppendEntries(entries)

	snapData := []byte("serialized-state")
	remaining := entries[3:] // entry 4 only; entries 1-3 are covered by the snapshot
	s.SaveSnapshot(3, 2, snapData, remaining)

	idx, term := s.SnapshotMeta()
	if idx != 3 || term != 2 {
		t.Fatalf("SnapshotMeta() = (%d, %d), want (3, 2)", idx, term)
	}
	if string(s.SnapshotData()) != string(snapData) {
		t.Fatalf("SnapshotData() = %q, want %q", s.SnapshotData(), snapData)
	}
	if len(s.Entries()) != 1 || s.Entries()[0].Index != 4 {
		t.Fatalf("Entries() after snapshot = %+v, want only index 4", s.Entries())
	}
	s.Close()

	s2 := openTestStorage(t, dir)
	idx, term = s2.SnapshotMeta()
	if idx != 3 || term != 2 {
		t.Fatalf("SnapshotMeta() after reopen = (%d, %d), want (3, 2)", idx, term)
	}
	if string(s2.SnapshotData()) != string(snapData) {
		t.Fatalf("SnapshotData() after reopen = %q, want %q", s2.SnapshotData(), snapData)
	}
	got := s2.Entries()
	if len(got) != 1 || got[0].Index != 4 || string(got[0].Data) != "d" {
		t.Fatalf("Entries() after reopen = %+v, want only index 4 data d", got)
	}
}
