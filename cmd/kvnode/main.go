// kvnode runs one member of the replicated key-value cluster.
//
// Example (3-node local cluster):
//
//	kvnode --id 1 --peers 1=localhost:7001,2=localhost:7002,3=localhost:7003 --data-dir data/n1
//	kvnode --id 2 --peers 1=localhost:7001,2=localhost:7002,3=localhost:7003 --data-dir data/n2
//	kvnode --id 3 --peers 1=localhost:7001,2=localhost:7002,3=localhost:7003 --data-dir data/n3
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/Roho9/raft-kv/server"
)

func main() {
	id := flag.Uint64("id", 0, "this node's id (must appear in --peers)")
	peersFlag := flag.String("peers", "", "comma-separated id=host:port for every cluster member")
	listen := flag.String("listen", "", "listen address (defaults to this node's --peers entry)")
	dataDir := flag.String("data-dir", "", "directory for persistent state")
	snapThreshold := flag.Int("snapshot-threshold", 8192, "log entries kept before compacting into a snapshot")
	fullFsync := flag.Bool("full-fsync", false, "force full disk cache flushes on WAL syncs (slower; survives power loss on a majority of nodes)")
	flag.Parse()

	if *id == 0 || *peersFlag == "" || *dataDir == "" {
		flag.Usage()
		os.Exit(2)
	}

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("bad --peers: %v", err)
	}
	self, ok := peers[*id]
	if !ok {
		log.Fatalf("--id %d not present in --peers", *id)
	}
	addr := *listen
	if addr == "" {
		// Bind to the port from the peers entry on all interfaces so it
		// works inside containers where the hostname resolves externally.
		_, port, err := splitHostPort(self)
		if err != nil {
			log.Fatalf("bad peer address %q: %v", self, err)
		}
		addr = ":" + port
	}

	logger := log.New(os.Stderr, fmt.Sprintf("[node %d] ", *id), log.LstdFlags|log.Lmicroseconds)
	node, err := server.NewNode(server.Config{
		ID:                *id,
		ListenAddr:        addr,
		Peers:             peers,
		DataDir:           *dataDir,
		FullFsync:         *fullFsync,
		SnapshotThreshold: *snapThreshold,
		Logger:            logger,
	})
	if err != nil {
		log.Fatalf("start node: %v", err)
	}
	if err := node.Start(); err != nil {
		log.Fatalf("serve: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	logger.Println("shutting down")
	node.Stop()
}

func parsePeers(s string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("entry %q is not id=addr", part)
		}
		n, err := strconv.ParseUint(id, 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("bad id in %q", part)
		}
		peers[n] = addr
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers given")
	}
	return peers, nil
}

func splitHostPort(addr string) (string, string, error) {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return "", "", fmt.Errorf("missing port")
	}
	return addr[:i], addr[i+1:], nil
}
