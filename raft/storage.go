package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/Roho9/raft-kv/gen/raftkvpb"
	"google.golang.org/protobuf/proto"
)

// Storage persists Raft state under a data directory:
//
//	meta.json     current term and vote (written atomically)
//	wal.log       append-only log entries, length and CRC framed
//	snapshot.bin  latest state machine snapshot with its log position
//
// Appends come in two flavors: AppendEntries fsyncs before returning
// (follower path, where the RPC response implies durability), while
// AppendNoSync plus a later Sync lets the leader group many proposals into
// one fsync (group commit). syncMu serializes fsyncs and file swaps so a
// sync never races a WAL rewrite; lock order is syncMu before mu.
type Storage struct {
	mu       sync.Mutex
	syncMu   sync.Mutex
	dir      string
	fullSync bool

	meta     Meta
	entries  []*pb.LogEntry
	snapIdx  uint64
	snapTerm uint64
	snapData []byte

	wal *os.File
}

type Meta struct {
	Term     uint64 `json:"term"`
	VotedFor uint64 `json:"voted_for"`
}

const (
	metaFile = "meta.json"
	walFile  = "wal.log"
	snapFile = "snapshot.bin"
)

// OpenStorage loads or creates persistent state in dir. With fullSync set,
// WAL syncs force a full disk cache flush (F_FULLFSYNC on darwin) instead
// of a regular fsync/fdatasync; see osSync for the trade-off.
func OpenStorage(dir string, fullSync bool) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Storage{dir: dir, fullSync: fullSync}
	if err := s.loadMeta(); err != nil {
		return nil, err
	}
	if err := s.loadSnapshot(); err != nil {
		return nil, err
	}
	if err := s.loadWAL(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, walFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.wal = f
	return s, nil
}

func (s *Storage) Close() {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal != nil {
		s.wal.Close()
		s.wal = nil
	}
}

func (s *Storage) Meta() Meta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta
}

func (s *Storage) Entries() []*pb.LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries
}

func (s *Storage) SnapshotMeta() (uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapIdx, s.snapTerm
}

func (s *Storage) SnapshotData() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapData
}

// SaveMeta durably records the current term and vote.
func (s *Storage) SaveMeta(term, votedFor uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta = Meta{Term: term, VotedFor: votedFor}
	b, _ := json.Marshal(s.meta)
	if err := atomicWrite(filepath.Join(s.dir, metaFile), b); err != nil {
		panic(fmt.Sprintf("raft storage: persist meta: %v", err))
	}
}

// AppendEntries appends a batch of entries to the WAL and fsyncs before
// returning. Used on the follower path, where the AppendEntries response
// tells the leader the entries are durable.
func (s *Storage) AppendEntries(entries []*pb.LogEntry) {
	if len(entries) == 0 {
		return
	}
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	f := s.appendLocked(entries)
	if err := s.syncWAL(f); err != nil {
		panic(fmt.Sprintf("raft storage: sync wal: %v", err))
	}
}

func (s *Storage) syncWAL(f *os.File) error {
	if s.fullSync {
		return f.Sync()
	}
	return osSync(f)
}

// AppendNoSync appends entries to the WAL without waiting for durability.
// The caller must not treat them as durable until a later Sync returns.
func (s *Storage) AppendNoSync(entries []*pb.LogEntry) {
	if len(entries) == 0 {
		return
	}
	s.appendLocked(entries)
}

func (s *Storage) appendLocked(entries []*pb.LogEntry) *os.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := make([]byte, 0, 256*len(entries))
	for _, e := range entries {
		buf = appendRecord(buf, e)
	}
	if _, err := s.wal.Write(buf); err != nil {
		panic(fmt.Sprintf("raft storage: append wal: %v", err))
	}
	s.entries = append(s.entries, entries...)
	return s.wal
}

// Sync fsyncs the WAL, making every previously appended entry durable.
// Many pending appends are covered by one call (group commit).
func (s *Storage) Sync() {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	f := s.wal
	s.mu.Unlock()
	if f == nil {
		return
	}
	if err := s.syncWAL(f); err != nil {
		panic(fmt.Sprintf("raft storage: sync wal: %v", err))
	}
}

// RewriteLog atomically replaces the WAL contents. Used after a follower
// truncates a conflicting suffix.
func (s *Storage) RewriteLog(entries []*pb.LogEntry) {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rewriteLogLocked(entries)
}

func (s *Storage) rewriteLogLocked(entries []*pb.LogEntry) {
	var buf []byte
	for _, e := range entries {
		buf = appendRecord(buf, e)
	}
	path := filepath.Join(s.dir, walFile)
	if err := atomicWrite(path, buf); err != nil {
		panic(fmt.Sprintf("raft storage: rewrite wal: %v", err))
	}
	if s.wal != nil {
		s.wal.Close()
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		panic(fmt.Sprintf("raft storage: reopen wal: %v", err))
	}
	s.wal = f
	s.entries = append([]*pb.LogEntry(nil), entries...)
}

// SaveSnapshot durably stores a snapshot and rewrites the WAL to hold only
// the entries that remain after compaction.
func (s *Storage) SaveSnapshot(index, term uint64, data []byte, remaining []*pb.LogEntry) {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := make([]byte, 16, 16+len(data))
	binary.LittleEndian.PutUint64(buf[0:8], index)
	binary.LittleEndian.PutUint64(buf[8:16], term)
	buf = append(buf, data...)
	if err := atomicWrite(filepath.Join(s.dir, snapFile), buf); err != nil {
		panic(fmt.Sprintf("raft storage: save snapshot: %v", err))
	}
	s.snapIdx, s.snapTerm, s.snapData = index, term, data
	s.rewriteLogLocked(remaining)
}

// ---------------- Loading ----------------

func (s *Storage) loadMeta() error {
	b, err := os.ReadFile(filepath.Join(s.dir, metaFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &s.meta)
}

func (s *Storage) loadSnapshot() error {
	b, err := os.ReadFile(filepath.Join(s.dir, snapFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(b) < 16 {
		return fmt.Errorf("snapshot file corrupt: %d bytes", len(b))
	}
	s.snapIdx = binary.LittleEndian.Uint64(b[0:8])
	s.snapTerm = binary.LittleEndian.Uint64(b[8:16])
	s.snapData = b[16:]
	return nil
}

// loadWAL reads entries, stopping at the first torn or corrupt record
// (which can only be an unacknowledged tail write from a crash).
func (s *Storage) loadWAL() error {
	path := filepath.Join(s.dir, walFile)
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	var offset int64
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		crc := binary.LittleEndian.Uint32(header[4:8])
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			break
		}
		if crc32.ChecksumIEEE(payload) != crc {
			break
		}
		var e pb.LogEntry
		if err := proto.Unmarshal(payload, &e); err != nil {
			break
		}
		// Skip entries already covered by the snapshot.
		if e.Index > s.snapIdx {
			s.entries = append(s.entries, &e)
		}
		offset += int64(8 + length)
	}
	return os.Truncate(path, offset)
}

// ---------------- Helpers ----------------

func appendRecord(buf []byte, e *pb.LogEntry) []byte {
	payload, err := proto.Marshal(e)
	if err != nil {
		panic(fmt.Sprintf("raft storage: marshal entry: %v", err))
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	buf = append(buf, header[:]...)
	return append(buf, payload...)
}

// atomicWrite writes data to path via a temp file, fsync, and rename so a
// crash never leaves a half-written file.
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err == nil {
		dir.Sync()
		dir.Close()
	}
	return nil
}
