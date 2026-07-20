// Package raft implements the Raft consensus algorithm: leader election,
// log replication, persistence, and log compaction via snapshots.
package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/Roho9/raft-kv/gen/raftkvpb"
)

type Role int32

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// ApplyMsg is delivered on the apply channel once an entry (or snapshot)
// is committed and ready to be applied to the state machine.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

type Config struct {
	ID                 uint64
	Peers              map[uint64]string // peer id -> address, excluding self
	DataDir            string
	FullFsync          bool // force full disk cache flushes on WAL syncs
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	Logger             *log.Logger
}

func (c *Config) withDefaults() {
	// Roughly 10x the heartbeat interval, matching common practice (etcd
	// defaults to the same ratio): tight enough for sub-second failover,
	// loose enough that a slow disk flush never triggers a spurious
	// election under load.
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 600 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
}

type Raft struct {
	mu        sync.Mutex
	cfg       Config
	id        uint64
	peerIDs   []uint64
	transport Transport
	storage   *Storage
	logger    *log.Logger

	// Persistent state (mirrored to storage on every change).
	term     uint64
	votedFor uint64         // 0 means no vote this term
	log      []*pb.LogEntry // log[0] is a sentinel at the snapshot boundary

	// Volatile state.
	commitIndex uint64
	lastApplied uint64
	role        Role
	leaderID    uint64

	// durable is the highest log index known to be fsynced locally. On the
	// leader, proposals are appended without an immediate sync and a
	// dedicated syncer goroutine batches fsyncs (group commit); an entry
	// only counts toward this node's replication quorum once durable.
	durable  uint64
	syncCond *sync.Cond

	// Leader state.
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	electionReset   time.Time
	electionTimeout time.Duration

	applyCh         chan ApplyMsg
	applyCond       *sync.Cond
	pendingSnapshot *ApplyMsg

	replCond *sync.Cond
	replGen  uint64

	killed bool
	doneWg sync.WaitGroup
}

// New restores a Raft node from stable storage (or bootstraps a fresh one)
// and starts its background goroutines. Committed entries are delivered on
// applyCh in order; the caller must drain it.
func New(cfg Config, transport Transport, applyCh chan ApplyMsg) (*Raft, error) {
	cfg.withDefaults()
	st, err := OpenStorage(cfg.DataDir, cfg.FullFsync)
	if err != nil {
		return nil, err
	}

	r := &Raft{
		cfg:       cfg,
		id:        cfg.ID,
		transport: transport,
		storage:   st,
		logger:    cfg.Logger,
		role:      Follower,
		applyCh:   applyCh,
	}
	for id := range cfg.Peers {
		r.peerIDs = append(r.peerIDs, id)
	}
	r.applyCond = sync.NewCond(&r.mu)
	r.replCond = sync.NewCond(&r.mu)
	r.syncCond = sync.NewCond(&r.mu)

	r.term = st.Meta().Term
	r.votedFor = st.Meta().VotedFor
	snapIdx, snapTerm := st.SnapshotMeta()
	r.log = append(r.log, &pb.LogEntry{Index: snapIdx, Term: snapTerm})
	r.log = append(r.log, st.Entries()...)
	r.commitIndex = snapIdx
	r.lastApplied = snapIdx
	if snapIdx > 0 {
		snap := st.SnapshotData()
		r.pendingSnapshot = &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snap,
			SnapshotIndex: snapIdx,
			SnapshotTerm:  snapTerm,
		}
	}
	r.durable = r.lastIndex()
	r.resetElectionTimer()

	r.doneWg.Add(3 + len(r.peerIDs))
	go r.electionTicker()
	go r.applier()
	go r.syncer()
	for _, peer := range r.peerIDs {
		go r.replicator(peer)
	}
	return r, nil
}

// Kill stops all background goroutines and closes storage.
func (r *Raft) Kill() {
	r.mu.Lock()
	r.killed = true
	r.applyCond.Broadcast()
	r.replCond.Broadcast()
	r.syncCond.Broadcast()
	r.mu.Unlock()
	r.doneWg.Wait()
	r.storage.Close()
}

func (r *Raft) isKilled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.killed
}

// ---------------- Public accessors ----------------

// State returns the current term and whether this node believes it is leader.
func (r *Raft) State() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.term, r.role == Leader
}

// LeaderID returns the id of the last known leader (0 if unknown).
func (r *Raft) LeaderID() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// LogEntryCount returns the number of entries currently held in memory
// (after the last snapshot). Used to decide when to compact.
func (r *Raft) LogEntryCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.log) - 1
}

// Propose appends a command to the log if this node is the leader.
// Returns the entry's index and term, and false if not leader.
func (r *Raft) Propose(data []byte) (uint64, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != Leader || r.killed {
		return 0, 0, false
	}
	entry := &pb.LogEntry{Index: r.lastIndex() + 1, Term: r.term, Data: data}
	r.log = append(r.log, entry)
	// Written but not yet synced: the syncer fsyncs a batch of proposals at
	// once, and only then does this entry count toward the quorum here.
	r.storage.AppendNoSync([]*pb.LogEntry{entry})
	r.syncCond.Broadcast()
	r.signalReplicators()
	return entry.Index, entry.Term, true
}

// syncer batches WAL fsyncs for leader proposals (group commit). Every
// proposal appended while one fsync is in flight is covered by the next,
// so one disk flush can commit many client operations.
func (r *Raft) syncer() {
	defer r.doneWg.Done()
	for {
		r.mu.Lock()
		for !r.killed && r.durable >= r.lastIndex() {
			r.syncCond.Wait()
		}
		if r.killed {
			r.mu.Unlock()
			return
		}
		target := r.lastIndex()
		r.mu.Unlock()

		r.storage.Sync()

		r.mu.Lock()
		if target > r.durable {
			r.durable = target
			if r.role == Leader {
				if r.durable > r.matchIndex[r.id] {
					r.matchIndex[r.id] = r.durable
				}
				r.advanceCommit()
				r.signalReplicators()
			}
		}
		r.mu.Unlock()
	}
}

// Snapshot tells Raft that the state machine has been serialized up to and
// including index, so the log prefix can be discarded.
func (r *Raft) Snapshot(index uint64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.firstIndex() || index > r.lastApplied {
		return
	}
	term := r.termAt(index)
	r.compactTo(index, term)
	r.storage.SaveSnapshot(index, term, data, r.tail())
}

// ---------------- Log helpers (callers hold r.mu) ----------------

func (r *Raft) firstIndex() uint64 { return r.log[0].Index }
func (r *Raft) lastIndex() uint64  { return r.log[len(r.log)-1].Index }
func (r *Raft) lastTerm() uint64   { return r.log[len(r.log)-1].Term }

func (r *Raft) entryAt(index uint64) *pb.LogEntry {
	return r.log[index-r.firstIndex()]
}

func (r *Raft) termAt(index uint64) uint64 {
	return r.entryAt(index).Term
}

// tail returns all real entries (everything after the sentinel).
func (r *Raft) tail() []*pb.LogEntry {
	return r.log[1:]
}

// compactTo drops log entries up to and including index, leaving a sentinel.
func (r *Raft) compactTo(index, term uint64) {
	kept := make([]*pb.LogEntry, 0, r.lastIndex()-index+1)
	kept = append(kept, &pb.LogEntry{Index: index, Term: term})
	for i := index + 1; i <= r.lastIndex(); i++ {
		kept = append(kept, r.entryAt(i))
	}
	r.log = kept
}

func (r *Raft) persistMeta() {
	r.storage.SaveMeta(r.term, r.votedFor)
}

// ---------------- Elections ----------------

func (r *Raft) resetElectionTimer() {
	r.electionReset = time.Now()
	spread := r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin
	r.electionTimeout = r.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(spread)+1))
}

func (r *Raft) electionTicker() {
	defer r.doneWg.Done()
	for !r.isKilled() {
		time.Sleep(10 * time.Millisecond)
		r.mu.Lock()
		if r.role != Leader && time.Since(r.electionReset) >= r.electionTimeout {
			r.startElection()
		}
		r.mu.Unlock()
	}
}

// startElection is called with r.mu held.
func (r *Raft) startElection() {
	r.role = Candidate
	r.term++
	r.votedFor = r.id
	r.persistMeta()
	r.resetElectionTimer()

	term := r.term
	req := &pb.RequestVoteRequest{
		Term:         term,
		CandidateId:  r.id,
		LastLogIndex: r.lastIndex(),
		LastLogTerm:  r.lastTerm(),
	}
	votes := 1
	majority := (len(r.peerIDs)+1)/2 + 1

	for _, peer := range r.peerIDs {
		go func(peer uint64) {
			ctx, cancel := context.WithTimeout(context.Background(), r.cfg.ElectionTimeoutMin)
			defer cancel()
			resp, err := r.transport.RequestVote(ctx, peer, req)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if resp.Term > r.term {
				r.stepDown(resp.Term)
				return
			}
			if r.role != Candidate || r.term != term || !resp.VoteGranted {
				return
			}
			votes++
			if votes >= majority {
				r.becomeLeader()
			}
		}(peer)
	}
}

// becomeLeader is called with r.mu held.
func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leaderID = r.id
	r.nextIndex = make(map[uint64]uint64)
	r.matchIndex = make(map[uint64]uint64)
	for _, peer := range r.peerIDs {
		r.nextIndex[peer] = r.lastIndex() + 1
		r.matchIndex[peer] = 0
	}
	r.matchIndex[r.id] = r.durable
	r.logger.Printf("raft[%d]: became leader for term %d", r.id, r.term)

	// Commit a no-op entry so entries from earlier terms become committable
	// immediately (Raft section 5.4.2). This also lets replicated reads
	// proceed promptly after an election.
	entry := &pb.LogEntry{Index: r.lastIndex() + 1, Term: r.term}
	r.log = append(r.log, entry)
	r.storage.AppendNoSync([]*pb.LogEntry{entry})
	r.syncCond.Broadcast()
	r.signalReplicators()
}

// stepDown is called with r.mu held when a higher term is observed.
func (r *Raft) stepDown(term uint64) {
	r.term = term
	r.role = Follower
	r.votedFor = 0
	r.persistMeta()
	r.resetElectionTimer()
}

// HandleRequestVote implements the RequestVote RPC receiver rules.
func (r *Raft) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term > r.term {
		r.stepDown(req.Term)
	}
	resp := &pb.RequestVoteResponse{Term: r.term}
	if req.Term < r.term {
		return resp
	}
	upToDate := req.LastLogTerm > r.lastTerm() ||
		(req.LastLogTerm == r.lastTerm() && req.LastLogIndex >= r.lastIndex())
	if (r.votedFor == 0 || r.votedFor == req.CandidateId) && upToDate {
		r.votedFor = req.CandidateId
		r.persistMeta()
		r.resetElectionTimer()
		resp.VoteGranted = true
	}
	return resp
}

// ---------------- Replication ----------------

// signalReplicators is called with r.mu held.
func (r *Raft) signalReplicators() {
	r.replGen++
	r.replCond.Broadcast()
}

// replicator drives AppendEntries / InstallSnapshot for one peer. It wakes
// on new proposals and on the heartbeat tick, and sends synchronously so a
// slow peer never accumulates unbounded in-flight RPCs.
func (r *Raft) replicator(peer uint64) {
	defer r.doneWg.Done()

	// Heartbeat ticker shared through replGen.
	stopHeartbeat := make(chan struct{})
	var hbWg sync.WaitGroup
	if peer == r.peerIDs[0] {
		hbWg.Add(1)
		go func() {
			defer hbWg.Done()
			ticker := time.NewTicker(r.cfg.HeartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stopHeartbeat:
					return
				case <-ticker.C:
					r.mu.Lock()
					if r.role == Leader {
						r.signalReplicators()
					}
					r.mu.Unlock()
				}
			}
		}()
	}
	defer func() {
		close(stopHeartbeat)
		hbWg.Wait()
	}()

	var seenGen uint64
	for {
		r.mu.Lock()
		for r.replGen == seenGen && !r.killed {
			r.replCond.Wait()
		}
		if r.killed {
			r.mu.Unlock()
			return
		}
		seenGen = r.replGen
		if r.role != Leader {
			r.mu.Unlock()
			continue
		}
		if r.nextIndex[peer] <= r.firstIndex() {
			r.sendSnapshot(peer)
		} else {
			r.sendAppend(peer)
		}
	}
}

// sendAppend sends one AppendEntries round to peer. Called with r.mu held;
// releases it while the RPC is in flight.
func (r *Raft) sendAppend(peer uint64) {
	term := r.term
	next := r.nextIndex[peer]
	prevIndex := next - 1
	prevTerm := r.termAt(prevIndex)
	entries := make([]*pb.LogEntry, r.lastIndex()-next+1)
	copy(entries, r.log[next-r.firstIndex():])
	req := &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     r.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.HeartbeatInterval*4)
	resp, err := r.transport.AppendEntries(ctx, peer, req)
	cancel()

	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil || r.role != Leader || r.term != term {
		return
	}
	if resp.Term > r.term {
		r.stepDown(resp.Term)
		return
	}
	if resp.Success {
		match := prevIndex + uint64(len(entries))
		if match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
		}
		if match+1 > r.nextIndex[peer] {
			r.nextIndex[peer] = match + 1
		}
		r.advanceCommit()
		return
	}
	// Conflict: back up nextIndex using the follower's hints.
	if resp.ConflictTerm != 0 {
		var found uint64
		for i := r.lastIndex(); i > r.firstIndex(); i-- {
			if r.termAt(i) == resp.ConflictTerm {
				found = i
				break
			}
			if r.termAt(i) < resp.ConflictTerm {
				break
			}
		}
		if found != 0 {
			r.nextIndex[peer] = found + 1
		} else {
			r.nextIndex[peer] = resp.ConflictIndex
		}
	} else {
		r.nextIndex[peer] = resp.ConflictIndex
	}
	if r.nextIndex[peer] < 1 {
		r.nextIndex[peer] = 1
	}
	r.signalReplicators()
}

// sendSnapshot ships the current snapshot to a peer whose log is too far
// behind. Called with r.mu held; releases it while the RPC is in flight.
func (r *Raft) sendSnapshot(peer uint64) {
	term := r.term
	snapIdx, snapTerm := r.storage.SnapshotMeta()
	req := &pb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          r.id,
		LastIncludedIndex: snapIdx,
		LastIncludedTerm:  snapTerm,
		Data:              r.storage.SnapshotData(),
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.HeartbeatInterval*20)
	resp, err := r.transport.InstallSnapshot(ctx, peer, req)
	cancel()

	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil || r.role != Leader || r.term != term {
		return
	}
	if resp.Term > r.term {
		r.stepDown(resp.Term)
		return
	}
	if snapIdx > r.matchIndex[peer] {
		r.matchIndex[peer] = snapIdx
	}
	if snapIdx+1 > r.nextIndex[peer] {
		r.nextIndex[peer] = snapIdx + 1
	}
}

// advanceCommit moves commitIndex forward when a majority has replicated an
// entry from the current term. Called with r.mu held.
func (r *Raft) advanceCommit() {
	majority := (len(r.peerIDs)+1)/2 + 1
	for n := r.lastIndex(); n > r.commitIndex; n-- {
		if r.termAt(n) != r.term {
			break
		}
		count := 0
		if r.matchIndex[r.id] >= n {
			count++
		}
		for _, peer := range r.peerIDs {
			if r.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= majority {
			r.commitIndex = n
			r.applyCond.Broadcast()
			break
		}
	}
}

// HandleAppendEntries implements the AppendEntries RPC receiver rules.
func (r *Raft) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	resp := &pb.AppendEntriesResponse{Term: r.term}
	if req.Term < r.term {
		return resp
	}
	if req.Term > r.term || r.role != Follower {
		r.stepDown(req.Term)
	}
	resp.Term = r.term
	r.leaderID = req.LeaderId
	r.resetElectionTimer()

	prevIndex := req.PrevLogIndex
	entries := req.Entries

	// Entries at or below the snapshot boundary are already committed here.
	if prevIndex < r.firstIndex() {
		skip := r.firstIndex() - prevIndex
		if uint64(len(entries)) <= skip {
			resp.Success = true
			return resp
		}
		entries = entries[skip:]
		prevIndex = r.firstIndex()
	}

	if prevIndex > r.lastIndex() {
		resp.ConflictIndex = r.lastIndex() + 1
		return resp
	}
	if r.termAt(prevIndex) != req.PrevLogTerm {
		conflictTerm := r.termAt(prevIndex)
		idx := prevIndex
		for idx > r.firstIndex() && r.termAt(idx-1) == conflictTerm {
			idx--
		}
		resp.ConflictTerm = conflictTerm
		resp.ConflictIndex = idx
		return resp
	}

	// Find the first entry that conflicts or is missing, truncate from
	// there, and append the remainder.
	appendFrom := -1
	for i, e := range entries {
		if e.Index > r.lastIndex() {
			appendFrom = i
			break
		}
		if r.termAt(e.Index) != e.Term {
			r.log = r.log[:e.Index-r.firstIndex()]
			r.storage.RewriteLog(r.tail())
			appendFrom = i
			break
		}
	}
	if appendFrom >= 0 {
		newEntries := entries[appendFrom:]
		r.log = append(r.log, newEntries...)
		// Synced before returning: our success response tells the leader
		// these entries are safely on disk here (the fsync also covers any
		// earlier not-yet-synced writes to the same file).
		r.storage.AppendEntries(newEntries)
	} else if r.durable < r.lastIndex() {
		// Nothing new to append, but a deposed leader can hold entries
		// appended with AppendNoSync that were never fsynced. A success
		// response asserts durability, so sync before claiming it.
		r.storage.Sync()
	}
	r.durable = r.lastIndex()

	resp.Success = true
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, r.lastIndex())
		r.applyCond.Broadcast()
	}
	return resp
}

// HandleInstallSnapshot implements the InstallSnapshot RPC receiver rules.
func (r *Raft) HandleInstallSnapshot(req *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	resp := &pb.InstallSnapshotResponse{Term: r.term}
	if req.Term < r.term {
		return resp
	}
	if req.Term > r.term || r.role != Follower {
		r.stepDown(req.Term)
	}
	resp.Term = r.term
	r.leaderID = req.LeaderId
	r.resetElectionTimer()

	if req.LastIncludedIndex <= r.commitIndex {
		return resp
	}

	// If our log extends past the snapshot and matches at the boundary,
	// keep the suffix; otherwise discard the whole log.
	if req.LastIncludedIndex < r.lastIndex() &&
		req.LastIncludedIndex >= r.firstIndex() &&
		r.termAt(req.LastIncludedIndex) == req.LastIncludedTerm {
		r.compactTo(req.LastIncludedIndex, req.LastIncludedTerm)
	} else {
		r.log = []*pb.LogEntry{{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm}}
	}
	r.storage.SaveSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data, r.tail())
	r.durable = r.lastIndex()

	r.commitIndex = req.LastIncludedIndex
	r.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotIndex: req.LastIncludedIndex,
		SnapshotTerm:  req.LastIncludedTerm,
	}
	r.applyCond.Broadcast()
	return resp
}

// ---------------- Applier ----------------

// applier delivers committed entries and installed snapshots to the state
// machine in log order.
func (r *Raft) applier() {
	defer r.doneWg.Done()
	for {
		r.mu.Lock()
		for !r.killed && r.pendingSnapshot == nil && r.lastApplied >= r.commitIndex {
			r.applyCond.Wait()
		}
		if r.killed {
			r.mu.Unlock()
			return
		}
		if r.pendingSnapshot != nil {
			msg := *r.pendingSnapshot
			r.pendingSnapshot = nil
			// The boot-time restore has SnapshotIndex equal to lastApplied
			// and must still be delivered, hence >= rather than >.
			if msg.SnapshotIndex >= r.lastApplied {
				r.lastApplied = msg.SnapshotIndex
				r.mu.Unlock()
				r.applyCh <- msg
			} else {
				r.mu.Unlock()
			}
			continue
		}
		batch := make([]ApplyMsg, 0, r.commitIndex-r.lastApplied)
		for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
			e := r.entryAt(i)
			batch = append(batch, ApplyMsg{
				CommandValid: true,
				Command:      e.Data,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			})
		}
		r.lastApplied = r.commitIndex
		r.mu.Unlock()
		for _, msg := range batch {
			r.applyCh <- msg
		}
	}
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
