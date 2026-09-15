package server

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync/atomic"
	"time"
)

// histBounds are the upper bounds (in seconds) of the request latency
// histogram buckets, chosen to resolve the sub-millisecond-to-100ms range
// this store actually operates in.
var histBounds = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1}

// opMetrics holds counters and a latency histogram for one operation type
// (get, put, delete). All fields are updated with atomics so requests never
// contend on a mutex for bookkeeping.
type opMetrics struct {
	success uint64
	failed  uint64
	sumNS   uint64 // total latency in nanoseconds, for _sum
	count   uint64 // total observations, for _count
	buckets []uint64
}

func newOpMetrics() *opMetrics {
	return &opMetrics{buckets: make([]uint64, len(histBounds))}
}

func (m *opMetrics) observe(d time.Duration, err error) {
	if err != nil {
		atomic.AddUint64(&m.failed, 1)
		return
	}
	atomic.AddUint64(&m.success, 1)
	atomic.AddUint64(&m.sumNS, uint64(d.Nanoseconds()))
	atomic.AddUint64(&m.count, 1)
	seconds := d.Seconds()
	idx := sort.SearchFloat64s(histBounds, seconds)
	if idx < len(m.buckets) {
		atomic.AddUint64(&m.buckets[idx], 1)
	}
	// The last bucket bound is finite; cumulative buckets below +Inf still
	// need every sample counted at +Inf, handled in render via cumulative sum.
}

// Metrics collects per-node operational counters and exposes them in
// Prometheus text exposition format. It is safe for concurrent use.
type Metrics struct {
	node *Node
	get  *opMetrics
	put  *opMetrics
	del  *opMetrics

	notLeaderTotal uint64
	timeoutTotal   uint64
}

func newMetrics(n *Node) *Metrics {
	return &Metrics{
		node: n,
		get:  newOpMetrics(),
		put:  newOpMetrics(),
		del:  newOpMetrics(),
	}
}

func (m *Metrics) recordNotLeader() { atomic.AddUint64(&m.notLeaderTotal, 1) }
func (m *Metrics) recordTimeout()   { atomic.AddUint64(&m.timeoutTotal, 1) }

func (m *Metrics) forOp(op string) *opMetrics {
	switch op {
	case "get":
		return m.get
	case "put":
		return m.put
	case "delete":
		return m.del
	default:
		return nil
	}
}

// Handler returns an http.Handler serving Prometheus text exposition format
// at whatever path it is mounted on (conventionally /metrics).
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.render(w)
	})
}

func (m *Metrics) render(w io.Writer) {
	term, isLeader := m.node.raft.State()
	leaderGauge := 0
	if isLeader {
		leaderGauge = 1
	}

	fmt.Fprintf(w, "# HELP raftkv_up 1 if the node is running.\n# TYPE raftkv_up gauge\nraftkv_up{node_id=\"%d\"} 1\n", m.node.cfg.ID)
	fmt.Fprintf(w, "# HELP raftkv_is_leader 1 if this node currently believes it is the Raft leader.\n# TYPE raftkv_is_leader gauge\nraftkv_is_leader{node_id=\"%d\"} %d\n", m.node.cfg.ID, leaderGauge)
	fmt.Fprintf(w, "# HELP raftkv_term Current Raft term.\n# TYPE raftkv_term gauge\nraftkv_term{node_id=\"%d\"} %d\n", m.node.cfg.ID, term)
	fmt.Fprintf(w, "# HELP raftkv_commit_index Highest committed Raft log index.\n# TYPE raftkv_commit_index gauge\nraftkv_commit_index{node_id=\"%d\"} %d\n", m.node.cfg.ID, m.node.raft.CommitIndex())
	fmt.Fprintf(w, "# HELP raftkv_log_entries In-memory log entries since the last snapshot.\n# TYPE raftkv_log_entries gauge\nraftkv_log_entries{node_id=\"%d\"} %d\n", m.node.cfg.ID, m.node.raft.LogEntryCount())
	fmt.Fprintf(w, "# HELP raftkv_keys Number of keys in the state machine.\n# TYPE raftkv_keys gauge\nraftkv_keys{node_id=\"%d\"} %d\n", m.node.cfg.ID, m.node.sm.Len())

	fmt.Fprintf(w, "# HELP raftkv_not_leader_redirects_total Requests rejected because this node was not leader.\n# TYPE raftkv_not_leader_redirects_total counter\nraftkv_not_leader_redirects_total{node_id=\"%d\"} %d\n", m.node.cfg.ID, atomic.LoadUint64(&m.notLeaderTotal))
	fmt.Fprintf(w, "# HELP raftkv_commit_timeouts_total Proposals that timed out waiting for commit.\n# TYPE raftkv_commit_timeouts_total counter\nraftkv_commit_timeouts_total{node_id=\"%d\"} %d\n", m.node.cfg.ID, atomic.LoadUint64(&m.timeoutTotal))

	fmt.Fprintln(w, "# HELP raftkv_requests_total Completed requests by operation and outcome.")
	fmt.Fprintln(w, "# TYPE raftkv_requests_total counter")
	fmt.Fprintln(w, "# HELP raftkv_request_duration_seconds Request latency for successful operations, end to end through Raft consensus.")
	fmt.Fprintln(w, "# TYPE raftkv_request_duration_seconds histogram")

	for _, op := range []string{"get", "put", "delete"} {
		om := m.forOp(op)
		fmt.Fprintf(w, "raftkv_requests_total{node_id=\"%d\",op=\"%s\",outcome=\"success\"} %d\n", m.node.cfg.ID, op, atomic.LoadUint64(&om.success))
		fmt.Fprintf(w, "raftkv_requests_total{node_id=\"%d\",op=\"%s\",outcome=\"error\"} %d\n", m.node.cfg.ID, op, atomic.LoadUint64(&om.failed))

		var cumulative uint64
		for i, bound := range histBounds {
			cumulative += atomic.LoadUint64(&om.buckets[i])
			fmt.Fprintf(w, "raftkv_request_duration_seconds_bucket{node_id=\"%d\",op=\"%s\",le=\"%g\"} %d\n", m.node.cfg.ID, op, bound, cumulative)
		}
		total := atomic.LoadUint64(&om.count)
		fmt.Fprintf(w, "raftkv_request_duration_seconds_bucket{node_id=\"%d\",op=\"%s\",le=\"+Inf\"} %d\n", m.node.cfg.ID, op, total)
		sumSeconds := float64(atomic.LoadUint64(&om.sumNS)) / 1e9
		fmt.Fprintf(w, "raftkv_request_duration_seconds_sum{node_id=\"%d\",op=\"%s\"} %g\n", m.node.cfg.ID, op, sumSeconds)
		fmt.Fprintf(w, "raftkv_request_duration_seconds_count{node_id=\"%d\",op=\"%s\"} %d\n", m.node.cfg.ID, op, total)
	}
}
