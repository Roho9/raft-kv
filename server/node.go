// Package server wires a Raft node, the KV state machine, and the gRPC
// services together into a runnable cluster node.
package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	pb "github.com/Roho9/raft-kv/gen/raftkvpb"
	"github.com/Roho9/raft-kv/kv"
	"github.com/Roho9/raft-kv/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const proposeTimeout = 3 * time.Second

type Config struct {
	ID                uint64
	ListenAddr        string            // address this node serves gRPC on
	Peers             map[uint64]string // id -> address for every node, including self
	DataDir           string
	FullFsync         bool   // see raft.Config.FullFsync
	SnapshotThreshold int    // compact once this many log entries accumulate; 0 disables
	MetricsAddr       string // if set, serve Prometheus metrics at http://MetricsAddr/metrics
	Logger            *log.Logger
}

// Node is one member of the cluster. It serves both the internal Raft
// service and the client-facing KV service on the same port.
type Node struct {
	pb.UnimplementedRaftServer
	pb.UnimplementedKVServer

	cfg        Config
	logger     *log.Logger
	raft       *raft.Raft
	transport  *raft.GRPCTransport
	sm         *kv.StateMachine
	applyCh    chan raft.ApplyMsg
	grpcSrv    *grpc.Server
	listener   net.Listener
	metrics    *Metrics
	metricsSrv *http.Server

	mu      sync.Mutex
	waiters map[uint64]chan applyOutcome

	applyDone chan struct{}
	serveErr  chan error
}

// applyOutcome is handed to a waiting client RPC once the log index it
// proposed at has been applied. The waiter verifies the command identity:
// if a different command landed at that index, leadership changed and the
// client must retry.
type applyOutcome struct {
	clientID string
	seq      uint64
	noop     bool
	result   kv.Result
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	peerAddrs := make(map[uint64]string)
	for id, addr := range cfg.Peers {
		if id != cfg.ID {
			peerAddrs[id] = addr
		}
	}

	n := &Node{
		cfg:       cfg,
		logger:    cfg.Logger,
		sm:        kv.NewStateMachine(),
		applyCh:   make(chan raft.ApplyMsg, 256),
		waiters:   make(map[uint64]chan applyOutcome),
		applyDone: make(chan struct{}),
		serveErr:  make(chan error, 1),
	}
	n.transport = raft.NewGRPCTransport(peerAddrs)

	r, err := raft.New(raft.Config{
		ID:        cfg.ID,
		Peers:     peerAddrs,
		DataDir:   cfg.DataDir,
		FullFsync: cfg.FullFsync,
		Logger:    cfg.Logger,
	}, n.transport, n.applyCh)
	if err != nil {
		return nil, err
	}
	n.raft = r
	n.metrics = newMetrics(n)
	return n, nil
}

// Start begins serving gRPC and applying committed entries.
func (n *Node) Start() error {
	lis, err := net.Listen("tcp", n.cfg.ListenAddr)
	if err != nil {
		return err
	}
	n.listener = lis
	n.grpcSrv = grpc.NewServer()
	pb.RegisterRaftServer(n.grpcSrv, n)
	pb.RegisterKVServer(n.grpcSrv, n)

	go n.applyLoop()
	go func() {
		n.serveErr <- n.grpcSrv.Serve(lis)
	}()
	n.logger.Printf("node[%d]: serving on %s", n.cfg.ID, lis.Addr())

	if n.cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", n.metrics.Handler())
		n.metricsSrv = &http.Server{Addr: n.cfg.MetricsAddr, Handler: mux}
		go func() {
			if err := n.metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				n.logger.Printf("node[%d]: metrics server: %v", n.cfg.ID, err)
			}
		}()
		n.logger.Printf("node[%d]: metrics on http://%s/metrics", n.cfg.ID, n.cfg.MetricsAddr)
	}
	return nil
}

// Addr returns the address the node is actually listening on.
func (n *Node) Addr() string {
	return n.listener.Addr().String()
}

// Transport exposes the Raft transport so tests can simulate partitions.
func (n *Node) Transport() *raft.GRPCTransport {
	return n.transport
}

// Stop shuts the node down. Safe to call once.
func (n *Node) Stop() {
	n.grpcSrv.Stop()
	if n.metricsSrv != nil {
		n.metricsSrv.Close()
	}
	// Kill Raft while the apply loop is still draining so the applier
	// goroutine can never block forever on the apply channel.
	n.raft.Kill()
	close(n.applyDone)
	n.transport.Close()
}

// ---------------- Apply loop ----------------

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.applyDone:
			return
		case msg := <-n.applyCh:
			n.applyOne(msg)
		}
	}
}

func (n *Node) applyOne(msg raft.ApplyMsg) {
	if msg.SnapshotValid {
		n.sm.Restore(msg.Snapshot)
		return
	}
	if !msg.CommandValid {
		return
	}

	outcome := applyOutcome{}
	if len(msg.Command) == 0 {
		// Leader no-op entry: nothing to apply.
		outcome.noop = true
	} else {
		var cmd pb.Command
		if err := proto.Unmarshal(msg.Command, &cmd); err != nil {
			n.logger.Printf("node[%d]: dropping undecodable entry %d: %v", n.cfg.ID, msg.CommandIndex, err)
			return
		}
		outcome.clientID = cmd.ClientId
		outcome.seq = cmd.Seq
		outcome.result = n.sm.Apply(&cmd)
	}

	n.mu.Lock()
	ch, ok := n.waiters[msg.CommandIndex]
	if ok {
		delete(n.waiters, msg.CommandIndex)
	}
	n.mu.Unlock()
	if ok {
		ch <- outcome
	}

	n.maybeSnapshot(msg.CommandIndex)
}

func (n *Node) maybeSnapshot(appliedIndex uint64) {
	if n.cfg.SnapshotThreshold <= 0 {
		return
	}
	if n.raft.LogEntryCount() >= n.cfg.SnapshotThreshold {
		n.raft.Snapshot(appliedIndex, n.sm.Snapshot())
	}
}

// ---------------- Proposals ----------------

const errNotLeader = "NOT_LEADER"

func (n *Node) notLeaderErr() error {
	hint := ""
	if id := n.raft.LeaderID(); id != 0 && id != n.cfg.ID {
		hint = n.cfg.Peers[id]
	}
	return status.Errorf(codes.FailedPrecondition, "%s leader_hint=%s", errNotLeader, hint)
}

// opName maps a command's op code to the lowercase label used in metrics.
func opName(op pb.Command_Op) string {
	switch op {
	case pb.Command_GET:
		return "get"
	case pb.Command_PUT:
		return "put"
	case pb.Command_DELETE:
		return "delete"
	default:
		return "unknown"
	}
}

// propose replicates cmd through Raft and waits for it to be applied.
func (n *Node) propose(ctx context.Context, cmd *pb.Command) (kv.Result, error) {
	start := time.Now()
	om := n.metrics.forOp(opName(cmd.Op))
	res, err := n.doPropose(ctx, cmd)
	if om != nil {
		om.observe(time.Since(start), err)
	}
	return res, err
}

func (n *Node) doPropose(ctx context.Context, cmd *pb.Command) (kv.Result, error) {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return kv.Result{}, status.Errorf(codes.Internal, "marshal command: %v", err)
	}
	// Register the waiter under the same lock the apply loop uses to look
	// it up, and before releasing it, so a fast commit can never deliver
	// the outcome before the waiter exists.
	n.mu.Lock()
	index, _, isLeader := n.raft.Propose(data)
	if !isLeader {
		n.mu.Unlock()
		n.metrics.recordNotLeader()
		return kv.Result{}, n.notLeaderErr()
	}
	ch := make(chan applyOutcome, 1)
	n.waiters[index] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.waiters, index)
		n.mu.Unlock()
	}()

	select {
	case out := <-ch:
		if out.noop || out.clientID != cmd.ClientId || out.seq != cmd.Seq {
			// A different command was committed at our index: this node
			// lost leadership before our entry was replicated.
			n.metrics.recordNotLeader()
			return kv.Result{}, n.notLeaderErr()
		}
		return out.result, nil
	case <-time.After(proposeTimeout):
		n.metrics.recordTimeout()
		return kv.Result{}, status.Errorf(codes.Unavailable, "commit timeout (no quorum or leadership lost)")
	case <-ctx.Done():
		return kv.Result{}, status.FromContextError(ctx.Err()).Err()
	}
}

// ---------------- KV service ----------------

// Get is linearizable: the read is replicated through the Raft log and
// executed at its commit point, so it always observes every write that
// completed before it.
func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	res, err := n.propose(ctx, &pb.Command{
		Op: pb.Command_GET, Key: req.Key, ClientId: req.ClientId, Seq: req.Seq,
	})
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: res.Value, Found: res.Found}, nil
}

func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	_, err := n.propose(ctx, &pb.Command{
		Op: pb.Command_PUT, Key: req.Key, Value: req.Value, ClientId: req.ClientId, Seq: req.Seq,
	})
	if err != nil {
		return nil, err
	}
	return &pb.PutResponse{}, nil
}

func (n *Node) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	res, err := n.propose(ctx, &pb.Command{
		Op: pb.Command_DELETE, Key: req.Key, ClientId: req.ClientId, Seq: req.Seq,
	})
	if err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{Existed: res.Existed}, nil
}

// ---------------- Raft service ----------------

// partitioned reports whether traffic from the given peer should be dropped
// to simulate a network partition (test only; no effect in production).
func (n *Node) partitioned(from uint64) error {
	if n.transport.IsBlocked(from) {
		return status.Errorf(codes.Unavailable, "partitioned from %d", from)
	}
	return nil
}

func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if err := n.partitioned(req.CandidateId); err != nil {
		return nil, err
	}
	return n.raft.HandleRequestVote(req), nil
}

func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if err := n.partitioned(req.LeaderId); err != nil {
		return nil, err
	}
	return n.raft.HandleAppendEntries(req), nil
}

func (n *Node) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if err := n.partitioned(req.LeaderId); err != nil {
		return nil, err
	}
	return n.raft.HandleInstallSnapshot(req), nil
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	_, isLeader := n.raft.State()
	return isLeader
}

// String describes the node for logs.
func (n *Node) String() string {
	return fmt.Sprintf("node-%d", n.cfg.ID)
}
