package raft

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains bounded-cardinality Prometheus instrumentation for one
// Raft node. The node ID is a constant label because each process owns exactly
// one consensus node.
type Metrics struct {
	roles             *prometheus.GaugeVec
	term              prometheus.Gauge
	commitIndex       prometheus.Gauge
	lastApplied       prometheus.Gauge
	logEntries        prometheus.Gauge
	elections         prometheus.Counter
	leadershipChanges prometheus.Counter
	proposals         *prometheus.CounterVec
	proposalDuration  *prometheus.HistogramVec
	peerRPCs          *prometheus.CounterVec
	peerRPCDuration   *prometheus.HistogramVec
	httpRequests      *prometheus.CounterVec
	httpDuration      *prometheus.HistogramVec
}

func NewMetrics(registerer prometheus.Registerer, nodeID string) *Metrics {
	labels := prometheus.Labels{"node": nodeID}
	metrics := &Metrics{
		roles: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "role",
			Help: "One-hot indicator for the node's current Raft role.", ConstLabels: labels,
		}, []string{"role"}),
		term: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "term",
			Help: "Current Raft term.", ConstLabels: labels,
		}),
		commitIndex: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "commit_index",
			Help: "Highest log index known to be committed.", ConstLabels: labels,
		}),
		lastApplied: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "last_applied",
			Help: "Highest log index applied to the state machine.", ConstLabels: labels,
		}),
		logEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "log_entries",
			Help: "Number of Raft log entries excluding the sentinel.", ConstLabels: labels,
		}),
		elections: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "elections_total",
			Help: "Election attempts started by this node.", ConstLabels: labels,
		}),
		leadershipChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "leadership_changes_total",
			Help: "Times this node became leader.", ConstLabels: labels,
		}),
		proposals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "proposals_total",
			Help: "Client and barrier proposals by operation and outcome.", ConstLabels: labels,
		}, []string{"operation", "outcome"}),
		proposalDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "proposal_duration_seconds",
			Help: "Time from proposal admission until commit or failure.", ConstLabels: labels,
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"operation", "outcome"}),
		peerRPCs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "peer_rpc_total",
			Help: "Outbound Raft RPCs by type, peer, and outcome.", ConstLabels: labels,
		}, []string{"rpc", "peer", "outcome"}),
		peerRPCDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "raft_kv", Subsystem: "node", Name: "peer_rpc_duration_seconds",
			Help: "Outbound Raft RPC latency.", ConstLabels: labels,
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		}, []string{"rpc", "peer", "outcome"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raft_kv", Subsystem: "http", Name: "requests_total",
			Help: "HTTP requests by stable route, method, and response status.", ConstLabels: labels,
		}, []string{"route", "method", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "raft_kv", Subsystem: "http", Name: "request_duration_seconds",
			Help: "HTTP request latency by stable route and method.", ConstLabels: labels,
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"route", "method"}),
	}
	registerer.MustRegister(
		metrics.roles, metrics.term, metrics.commitIndex, metrics.lastApplied,
		metrics.logEntries, metrics.elections, metrics.leadershipChanges,
		metrics.proposals, metrics.proposalDuration, metrics.peerRPCs,
		metrics.peerRPCDuration, metrics.httpRequests, metrics.httpDuration,
	)
	return metrics
}

func (m *Metrics) setState(role Role, term, commitIndex, lastApplied, lastLogIndex uint64) {
	if m == nil {
		return
	}
	for _, candidate := range []Role{Follower, Candidate, Leader} {
		value := 0.0
		if role == candidate {
			value = 1
		}
		m.roles.WithLabelValues(string(candidate)).Set(value)
	}
	m.term.Set(float64(term))
	m.commitIndex.Set(float64(commitIndex))
	m.lastApplied.Set(float64(lastApplied))
	m.logEntries.Set(float64(lastLogIndex))
}

func (m *Metrics) observeProposal(operation, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.proposals.WithLabelValues(operation, outcome).Inc()
	m.proposalDuration.WithLabelValues(operation, outcome).Observe(duration.Seconds())
}

func (m *Metrics) observePeerRPC(rpc, peer, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.peerRPCs.WithLabelValues(rpc, peer, outcome).Inc()
	m.peerRPCDuration.WithLabelValues(rpc, peer, outcome).Observe(duration.Seconds())
}

func (m *Metrics) observeHTTP(route, method string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	m.httpRequests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(route, method).Observe(duration.Seconds())
}
