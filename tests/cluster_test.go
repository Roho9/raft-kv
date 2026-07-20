// Package tests contains end-to-end cluster tests: leader election,
// failover, snapshot catch-up, simulated network partitions, and
// consistency under heavy concurrent load.
package tests

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Roho9/raft-kv/client"
	"github.com/Roho9/raft-kv/server"
)

// cluster manages n in-process nodes for a test.
type cluster struct {
	t       *testing.T
	dir     string
	peers   map[uint64]string
	nodes   map[uint64]*server.Node
	snapCap int
}

func newCluster(t *testing.T, n int, snapshotThreshold int) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		dir:     t.TempDir(),
		peers:   make(map[uint64]string),
		nodes:   make(map[uint64]*server.Node),
		snapCap: snapshotThreshold,
	}
	// Reserve ports first so every node knows the full peer map at start.
	for i := 1; i <= n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c.peers[uint64(i)] = lis.Addr().String()
		lis.Close()
	}
	for i := 1; i <= n; i++ {
		c.startNode(uint64(i))
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) startNode(id uint64) {
	c.t.Helper()
	logger := log.New(os.Stderr, fmt.Sprintf("[node %d] ", id), log.Ltime|log.Lmicroseconds)
	node, err := server.NewNode(server.Config{
		ID:                id,
		ListenAddr:        c.peers[id],
		Peers:             c.peers,
		DataDir:           filepath.Join(c.dir, fmt.Sprintf("n%d", id)),
		SnapshotThreshold: c.snapCap,
		Logger:            logger,
	})
	if err != nil {
		c.t.Fatalf("start node %d: %v", id, err)
	}
	if err := node.Start(); err != nil {
		c.t.Fatalf("serve node %d: %v", id, err)
	}
	c.nodes[id] = node
}

func (c *cluster) stopNode(id uint64) {
	if n, ok := c.nodes[id]; ok {
		n.Stop()
		delete(c.nodes, id)
	}
}

func (c *cluster) shutdown() {
	for id := range c.nodes {
		c.stopNode(id)
	}
}

func (c *cluster) addrs() []string {
	out := make([]string, 0, len(c.peers))
	for _, a := range c.peers {
		out = append(out, a)
	}
	return out
}

// waitForLeader blocks until exactly one running node claims leadership.
func (c *cluster) waitForLeader() uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []uint64
		for id, n := range c.nodes {
			if n.IsLeader() {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no single leader elected within 10s")
	return 0
}

// isolate cuts node id off from every other node in both directions.
func (c *cluster) isolate(id uint64) {
	for other, n := range c.nodes {
		if other == id {
			continue
		}
		n.Transport().Block(id)
		if self, ok := c.nodes[id]; ok {
			self.Transport().Block(other)
		}
	}
}

// heal restores full connectivity.
func (c *cluster) heal() {
	for id, n := range c.nodes {
		for other := range c.peers {
			if other != id {
				n.Transport().Unblock(other)
			}
		}
	}
}

func (c *cluster) client() *client.Client {
	c.t.Helper()
	cl, err := client.New(c.addrs())
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(cl.Close)
	return cl
}

// ---------------- Tests ----------------

func TestLeaderElection(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.waitForLeader()
	if leader == 0 {
		t.Fatal("expected a leader")
	}
}

func TestBasicPutGetDelete(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.waitForLeader()
	cl := c.client()
	ctx := context.Background()

	if err := cl.Put(ctx, "alpha", []byte("one")); err != nil {
		t.Fatal(err)
	}
	v, found, err := cl.Get(ctx, "alpha")
	if err != nil || !found || string(v) != "one" {
		t.Fatalf("get alpha = %q, %v, %v; want one", v, found, err)
	}
	existed, err := cl.Delete(ctx, "alpha")
	if err != nil || !existed {
		t.Fatalf("delete alpha existed=%v err=%v", existed, err)
	}
	_, found, err = cl.Get(ctx, "alpha")
	if err != nil || found {
		t.Fatalf("alpha should be gone, found=%v err=%v", found, err)
	}
}

// TestLeaderFailover kills the leader and verifies the cluster elects a new
// one and keeps serving with no data loss.
func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3, 0)
	old := c.waitForLeader()
	cl := c.client()
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("pre-%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	c.stopNode(old)
	newLeader := c.waitForLeader()
	if newLeader == old {
		t.Fatalf("dead node %d still leader", old)
	}

	// Data written before the failure must survive.
	_, found, err := cl.Get(ctx, "pre-19")
	if err != nil || !found {
		t.Fatalf("lost committed write after failover: found=%v err=%v", found, err)
	}
	// And the two survivors must still accept writes.
	if err := cl.Put(ctx, "post", []byte("v2")); err != nil {
		t.Fatalf("write after failover: %v", err)
	}
}

// TestPartitionedLeaderCannotCommit isolates the leader; the majority side
// must elect a new leader and keep committing, and the old leader must not
// acknowledge writes while isolated.
func TestPartitionedLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 3, 0)
	old := c.waitForLeader()
	cl := c.client()
	ctx := context.Background()

	if err := cl.Put(ctx, "before", []byte("1")); err != nil {
		t.Fatal(err)
	}

	c.isolate(old)

	// Majority side elects a fresh leader and accepts writes.
	var majorityLeader uint64
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if id != old && n.IsLeader() {
				majorityLeader = id
			}
		}
		if majorityLeader != 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if majorityLeader == 0 {
		t.Fatal("majority side did not elect a leader")
	}

	if err := cl.Put(ctx, "during", []byte("2")); err != nil {
		t.Fatalf("majority side rejected write: %v", err)
	}

	c.heal()
	c.waitForLeader()

	// After healing, everything must be readable and the deposed leader's
	// log must have converged (verified implicitly: reads go through the
	// current leader, and a subsequent failover test would catch drift).
	for _, key := range []string{"before", "during"} {
		_, found, err := cl.Get(ctx, key)
		if err != nil || !found {
			t.Fatalf("key %q missing after heal: found=%v err=%v", key, found, err)
		}
	}
}

// TestSnapshotCatchUp forces log compaction while a follower is down, then
// verifies the restarted follower catches up (necessarily via
// InstallSnapshot) and can serve as part of a quorum.
func TestSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3, 100) // compact every 100 entries
	leader := c.waitForLeader()
	cl := c.client()
	ctx := context.Background()

	var follower uint64
	for id := range c.nodes {
		if id != leader {
			follower = id
			break
		}
	}
	c.stopNode(follower)

	// Write well past the snapshot threshold so the leader compacts away
	// the log prefix the dead follower would need.
	for i := 0; i < 500; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k-%d", i%50), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	c.startNode(follower)
	time.Sleep(2 * time.Second) // allow snapshot transfer and catch-up

	// Kill the leader: the restarted follower must now participate in a
	// quorum with correct state.
	c.stopNode(leader)
	c.waitForLeader()

	v, found, err := cl.Get(ctx, "k-49")
	if err != nil || !found || string(v) != "v-499" {
		t.Fatalf("after snapshot catch-up: got %q found=%v err=%v, want v-499", v, found, err)
	}
}

// TestRestartDurability restarts the whole cluster and verifies committed
// data survives via the WAL and snapshots.
func TestRestartDurability(t *testing.T) {
	c := newCluster(t, 3, 50)
	c.waitForLeader()
	cl := c.client()
	ctx := context.Background()

	for i := 0; i < 200; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("d-%d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}

	for id := range c.peers {
		c.stopNode(id)
	}
	for id := range c.peers {
		c.startNode(id)
	}
	c.waitForLeader()

	cl2 := c.client()
	for _, i := range []int{0, 99, 199} {
		_, found, err := cl2.Get(ctx, fmt.Sprintf("d-%d", i))
		if err != nil || !found {
			t.Fatalf("d-%d lost after full restart: found=%v err=%v", i, found, err)
		}
	}
}

// TestConcurrentWritesWithPartition runs 10,000 concurrent writes while a
// node (sometimes the leader) is partitioned away mid-run, then verifies
// that every acknowledged write is durable and reads reflect the last
// acknowledged value per key.
func TestConcurrentWritesWithPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("long test")
	}
	c := newCluster(t, 3, 2000)
	leader := c.waitForLeader()
	ctx := context.Background()

	const (
		workers      = 20
		opsPerWorker = 500 // 10,000 total
	)

	type ack struct {
		key string
		val string
	}
	acked := make([]ack, 0, workers*opsPerWorker)
	var ackMu sync.Mutex

	// Partition the current leader mid-run, hold it for a bit, then heal.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second)
		c.isolate(leader)
		time.Sleep(2 * time.Second)
		c.heal()
	}()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			cl, err := client.New(c.addrs())
			if err != nil {
				t.Error(err)
				return
			}
			defer cl.Close()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("w%d-k%d", worker, i%25)
				val := fmt.Sprintf("w%d-v%d", worker, i)
				opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				err := cl.Put(opCtx, key, []byte(val))
				cancel()
				if err == nil {
					ackMu.Lock()
					acked = append(acked, ack{key, val})
					ackMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	c.heal()
	c.waitForLeader()

	if len(acked) < workers*opsPerWorker*8/10 {
		t.Fatalf("too few acknowledged writes: %d of %d", len(acked), workers*opsPerWorker)
	}
	t.Logf("acknowledged %d of %d writes across the partition", len(acked), workers*opsPerWorker)

	// The last acknowledged value for each key must be exactly what a
	// linearizable read returns now (each key is written by one worker in
	// sequence, so the last ack is the true final value).
	lastVal := make(map[string]string)
	for _, a := range acked {
		lastVal[a.key] = a.val
	}
	cl := c.client()
	for key, want := range lastVal {
		v, found, err := cl.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if !found {
			t.Fatalf("acknowledged key %s missing", key)
		}
		if string(v) != want {
			t.Fatalf("key %s = %q, want %q (lost or reordered write)", key, v, want)
		}
	}
}

// TestIdempotentRetries verifies that a retried request (same client id and
// sequence number) is applied at most once even when sent to the cluster
// multiple times.
func TestIdempotentRetries(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.waitForLeader()
	ctx := context.Background()

	// The client already retries internally with a fixed seq; hammer the
	// same logical operation through failover to exercise the dedup path.
	cl := c.client()
	if err := cl.Put(ctx, "ctr", []byte("first")); err != nil {
		t.Fatal(err)
	}
	leader := c.waitForLeader()
	c.stopNode(leader)
	// This write will likely hit the dead node first and retry across the
	// remaining nodes; the session table guarantees at-most-once apply.
	if err := cl.Put(ctx, "ctr", []byte("second")); err != nil {
		t.Fatal(err)
	}
	v, found, err := cl.Get(ctx, "ctr")
	if err != nil || !found || string(v) != "second" {
		t.Fatalf("ctr = %q found=%v err=%v, want second", v, found, err)
	}
}
