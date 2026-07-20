package raft

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/Roho9/raft-kv/gen/raftkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport carries Raft RPCs between peers. Block and Unblock simulate
// network partitions for testing: a blocked peer's messages are dropped in
// both directions (the RPC handlers consult IsBlocked on receive).
type Transport interface {
	RequestVote(ctx context.Context, to uint64, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to uint64, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, to uint64, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
	Block(id uint64)
	Unblock(id uint64)
	IsBlocked(id uint64) bool
	Close()
}

// GRPCTransport dials peers lazily and reuses connections.
type GRPCTransport struct {
	mu      sync.Mutex
	addrs   map[uint64]string
	conns   map[uint64]*grpc.ClientConn
	clients map[uint64]pb.RaftClient
	blocked map[uint64]bool
}

func NewGRPCTransport(addrs map[uint64]string) *GRPCTransport {
	return &GRPCTransport{
		addrs:   addrs,
		conns:   make(map[uint64]*grpc.ClientConn),
		clients: make(map[uint64]pb.RaftClient),
		blocked: make(map[uint64]bool),
	}
}

func (t *GRPCTransport) client(to uint64) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[to]; ok {
		return c, nil
	}
	addr, ok := t.addrs[to]
	if !ok {
		return nil, fmt.Errorf("unknown peer %d", to)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[to] = conn
	c := pb.NewRaftClient(conn)
	t.clients[to] = c
	return c, nil
}

func (t *GRPCTransport) checkBlocked(to uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.blocked[to] {
		return fmt.Errorf("peer %d unreachable (partitioned)", to)
	}
	return nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, to uint64, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if err := t.checkBlocked(to); err != nil {
		return nil, err
	}
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, req)
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, to uint64, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if err := t.checkBlocked(to); err != nil {
		return nil, err
	}
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, req)
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, to uint64, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if err := t.checkBlocked(to); err != nil {
		return nil, err
	}
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	return c.InstallSnapshot(ctx, req)
}

func (t *GRPCTransport) Block(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked[id] = true
}

func (t *GRPCTransport) Unblock(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.blocked, id)
}

func (t *GRPCTransport) IsBlocked(id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.blocked[id]
}

func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		conn.Close()
	}
	t.conns = make(map[uint64]*grpc.ClientConn)
	t.clients = make(map[uint64]pb.RaftClient)
}
