// Package client provides a cluster-aware KV client with idempotent
// read/write semantics. Every operation carries a unique client id and a
// monotonically increasing sequence number; the replicated state machine
// applies each (client, seq) pair at most once, so the client can safely
// retry an operation across leader failovers without it executing twice.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Roho9/raft-kv/gen/raftkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	rpcTimeout  = 4 * time.Second
	retryPause  = 50 * time.Millisecond
	maxAttempts = 40
)

type Client struct {
	addrs    []string
	clientID string
	seq      atomic.Uint64

	mu     sync.Mutex
	leader int // index into addrs of the presumed leader
	conns  map[string]*grpc.ClientConn
	stubs  map[string]pb.KVClient
}

// New creates a client for a cluster reachable at the given addresses.
func New(addrs []string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no cluster addresses given")
	}
	idBytes := make([]byte, 8)
	if _, err := rand.Read(idBytes); err != nil {
		return nil, err
	}
	return &Client{
		addrs:    addrs,
		clientID: hex.EncodeToString(idBytes),
		conns:    make(map[string]*grpc.ClientConn),
		stubs:    make(map[string]pb.KVClient),
	}, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
}

func (c *Client) stub(addr string) (pb.KVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.stubs[addr]; ok {
		return s, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	s := pb.NewKVClient(conn)
	c.stubs[addr] = s
	return s, nil
}

func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addrs[c.leader]
}

func (c *Client) rotate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = (c.leader + 1) % len(c.addrs)
}

func (c *Client) followHint(hint string) {
	if hint == "" {
		c.rotate()
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, a := range c.addrs {
		if a == hint {
			c.leader = i
			return
		}
	}
	c.leader = (c.leader + 1) % len(c.addrs)
}

// call retries op against the cluster until it succeeds, following leader
// hints and rotating through nodes on failure. Because the sequence number
// is fixed for the lifetime of one logical operation, retries are
// idempotent even if an earlier attempt actually committed.
func (c *Client) call(ctx context.Context, op func(ctx context.Context, stub pb.KVClient) error) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		addr := c.target()
		stub, err := c.stub(addr)
		if err != nil {
			lastErr = err
			c.rotate()
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
		err = op(rpcCtx, stub)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.FailedPrecondition && strings.HasPrefix(st.Message(), "NOT_LEADER") {
			c.followHint(parseHint(st.Message()))
		} else {
			c.rotate()
		}
		time.Sleep(retryPause)
	}
	return fmt.Errorf("operation failed after %d attempts: %w", maxAttempts, lastErr)
}

func parseHint(msg string) string {
	const marker = "leader_hint="
	i := strings.Index(msg, marker)
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(msg[i+len(marker):])
}

// Get returns the value for key. The read is linearizable.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	req := &pb.GetRequest{Key: key, ClientId: c.clientID, Seq: c.seq.Add(1)}
	var resp *pb.GetResponse
	err := c.call(ctx, func(ctx context.Context, stub pb.KVClient) error {
		var err error
		resp, err = stub.Get(ctx, req)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// Put stores value under key. Exactly-once even across retries.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	req := &pb.PutRequest{Key: key, Value: value, ClientId: c.clientID, Seq: c.seq.Add(1)}
	return c.call(ctx, func(ctx context.Context, stub pb.KVClient) error {
		_, err := stub.Put(ctx, req)
		return err
	})
}

// Delete removes key, reporting whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	req := &pb.DeleteRequest{Key: key, ClientId: c.clientID, Seq: c.seq.Add(1)}
	var resp *pb.DeleteResponse
	err := c.call(ctx, func(ctx context.Context, stub pb.KVClient) error {
		var err error
		resp, err = stub.Delete(ctx, req)
		return err
	})
	if err != nil {
		return false, err
	}
	return resp.Existed, nil
}
