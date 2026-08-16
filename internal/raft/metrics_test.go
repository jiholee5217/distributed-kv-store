package raft

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jiholee5217/distributed-kv-store/internal/statemachine"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsTrackLeadershipProposalAndHTTP(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	metrics := NewMetrics(registry, "solo")
	node, err := NewNode(Config{
		ID:                 "solo",
		Members:            []Member{{ID: "solo", URL: "http://127.0.0.1:1"}},
		Storage:            NewMemoryStorage(),
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
		Metrics:            metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	t.Cleanup(node.Close)
	waitForLeader(t, []*Node{node}, time.Second)
	if _, _, err := node.Propose(t.Context(), statemachine.Command{
		Operation: statemachine.OperationPut, Key: "metrics", Value: "work",
	}); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	response := httptest.NewRecorder()
	node.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status endpoint returned %d", response.Code)
	}

	if got := testutil.ToFloat64(metrics.roles.WithLabelValues(string(Leader))); got != 1 {
		t.Fatalf("leader role gauge = %v; want 1", got)
	}
	if got := testutil.ToFloat64(metrics.elections); got < 1 {
		t.Fatalf("elections counter = %v; want at least 1", got)
	}
	if got := testutil.ToFloat64(metrics.leadershipChanges); got != 1 {
		t.Fatalf("leadership changes = %v; want 1", got)
	}
	if got := testutil.ToFloat64(metrics.proposals.WithLabelValues("put", "committed")); got != 1 {
		t.Fatalf("committed put proposals = %v; want 1", got)
	}
	if got := testutil.ToFloat64(metrics.httpRequests.WithLabelValues("/v1/status", http.MethodGet, "200")); got != 1 {
		t.Fatalf("status request counter = %v; want 1", got)
	}
	if _, err := registry.Gather(); err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
}
