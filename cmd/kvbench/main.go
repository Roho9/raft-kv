// kvbench load-tests a running cluster and reports throughput and latency
// percentiles.
//
// Example:
//
//	kvbench --addrs localhost:7001,localhost:7002,localhost:7003 \
//	        --clients 64 --ops 50000 --read-ratio 0.5 --value-size 128
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Roho9/raft-kv/client"
)

func main() {
	addrsFlag := flag.String("addrs", "localhost:7001,localhost:7002,localhost:7003", "comma-separated cluster addresses")
	clients := flag.Int("clients", 64, "number of concurrent client sessions")
	totalOps := flag.Int("ops", 50000, "total operations to run")
	readRatio := flag.Float64("read-ratio", 0.5, "fraction of operations that are reads")
	valueSize := flag.Int("value-size", 128, "value payload size in bytes")
	keySpace := flag.Int("keys", 1000, "number of distinct keys")
	flag.Parse()

	addrs := strings.Split(*addrsFlag, ",")
	opsPerClient := *totalOps / *clients
	value := make([]byte, *valueSize)
	rand.Read(value)

	fmt.Printf("benchmark: %d clients x %d ops (%.0f%% reads), %dB values, %d keys\n",
		*clients, opsPerClient, *readRatio*100, *valueSize, *keySpace)

	var mu sync.Mutex
	allLatencies := make([]time.Duration, 0, *totalOps)
	var wg sync.WaitGroup
	var failed int64

	start := time.Now()
	for w := 0; w < *clients; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			c, err := client.New(addrs)
			if err != nil {
				log.Printf("worker %d: %v", worker, err)
				return
			}
			defer c.Close()
			rng := rand.New(rand.NewSource(int64(worker)))
			lats := make([]time.Duration, 0, opsPerClient)
			ctx := context.Background()
			for i := 0; i < opsPerClient; i++ {
				key := fmt.Sprintf("key-%d", rng.Intn(*keySpace))
				t0 := time.Now()
				var err error
				if rng.Float64() < *readRatio {
					_, _, err = c.Get(ctx, key)
				} else {
					err = c.Put(ctx, key, value)
				}
				if err != nil {
					mu.Lock()
					failed++
					mu.Unlock()
					continue
				}
				lats = append(lats, time.Since(t0))
			}
			mu.Lock()
			allLatencies = append(allLatencies, lats...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if len(allLatencies) == 0 {
		log.Fatal("no operations succeeded")
	}
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	pct := func(p float64) time.Duration {
		idx := int(p * float64(len(allLatencies)-1))
		return allLatencies[idx]
	}

	fmt.Printf("\ncompleted %d ops in %s (%d failed)\n", len(allLatencies), elapsed.Round(time.Millisecond), failed)
	fmt.Printf("throughput: %.0f ops/sec\n", float64(len(allLatencies))/elapsed.Seconds())
	fmt.Printf("latency p50: %s\n", pct(0.50).Round(10*time.Microsecond))
	fmt.Printf("latency p95: %s\n", pct(0.95).Round(10*time.Microsecond))
	fmt.Printf("latency p99: %s\n", pct(0.99).Round(10*time.Microsecond))
	fmt.Printf("latency max: %s\n", allLatencies[len(allLatencies)-1].Round(10*time.Microsecond))
}
