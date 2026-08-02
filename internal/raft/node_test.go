package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jiholee5217/distributed-kv-store/internal/statemachine"
)

func TestFiveNodeClusterElectsLeaderReplicatesAndFailsOver(t *testing.T) {
	const clusterSize = 5
	servers := make([]*httptest.Server, clusterSize)
	members := make([]Member, clusterSize)
	for index := range servers {
		servers[index] = httptest.NewUnstartedServer(nil)
		members[index] = Member{
			ID:  fmt.Sprintf("node-%d", index+1),
			URL: "http://" + servers[index].Listener.Addr().String(),
		}
	}

	nodes := make([]*Node, clusterSize)
	for index := range nodes {
		node, err := NewNode(Config{
			ID:                 members[index].ID,
			Members:            members,
			Storage:            NewMemoryStorage(),
			ElectionTimeoutMin: 140 * time.Millisecond,
			ElectionTimeoutMax: 280 * time.Millisecond,
			HeartbeatInterval:  40 * time.Millisecond,
			RPCTimeout:         100 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[index] = node
		servers[index].Config.Handler = node.Handler()
		servers[index].Start()
	}
	for _, node := range nodes {
		node.Start()
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			if node != nil {
				node.Close()
			}
		}
		for _, server := range servers {
			server.Close()
		}
	})

	leader := waitForLeader(t, nodes, 5*time.Second)
	waitForLeaderKnowledge(t, nodes, leader, 2*time.Second)
	follower := firstAliveExcept(nodes, leader)
	putValue(t, servers[follower].URL, "project", "raft", http.StatusCreated)

	reader := firstAliveExcept(nodes, follower)
	if got := getValue(t, servers[reader].URL, "project", http.StatusOK); got != "raft" {
		t.Fatalf("GET value = %q; want raft", got)
	}
	waitForAppliedValue(t, nodes, "project", "raft", 5*time.Second)

	nodes[leader].Close()
	nodes[leader] = nil
	servers[leader].Close()
	newLeader := waitForLeader(t, nodes, 5*time.Second)
	if newLeader == leader {
		t.Fatal("failed node was re-elected")
	}
	waitForLeaderKnowledge(t, nodes, newLeader, 2*time.Second)
	forwarder := firstAliveExcept(nodes, newLeader)
	putValue(t, servers[forwarder].URL, "project", "failover", http.StatusCreated)
	if got := getValue(t, servers[newLeader].URL, "project", http.StatusOK); got != "failover" {
		t.Fatalf("GET after failover = %q; want failover", got)
	}
}

func TestNodeReplaysCommittedLogOnRestart(t *testing.T) {
	storage := NewMemoryStorage()
	members := []Member{{ID: "solo", URL: "http://127.0.0.1:1"}}
	node, err := NewNode(Config{
		ID:                 "solo",
		Members:            members,
		Storage:            storage,
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	waitForLeader(t, []*Node{node}, time.Second)
	if _, _, err := node.Propose(t.Context(), statemachine.Command{
		Operation: statemachine.OperationPut,
		Key:       "durable",
		Value:     "yes",
	}); err != nil {
		t.Fatal(err)
	}
	node.Close()

	restarted, err := NewNode(Config{
		ID:                 "solo",
		Members:            members,
		Storage:            storage,
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := restarted.Get("durable")
	if !ok || value != "yes" {
		t.Fatalf("replayed value = %q, %v; want yes, true", value, ok)
	}
}

func TestRequestVoteRejectsStaleCandidate(t *testing.T) {
	storage := NewMemoryStorage()
	node, err := NewNode(Config{
		ID:      "a",
		Members: []Member{{ID: "a", URL: "http://a"}, {ID: "b", URL: "http://b"}, {ID: "c", URL: "http://c"}},
		Storage: storage,
	})
	if err != nil {
		t.Fatal(err)
	}
	node.mu.Lock()
	node.currentTerm = 4
	node.log = append(node.log, Entry{Index: 1, Term: 4, Command: statemachine.Command{Operation: statemachine.OperationBarrier}})
	if err := node.persistLocked(); err != nil {
		t.Fatal(err)
	}
	node.mu.Unlock()

	response, err := node.HandleRequestVote(RequestVoteRequest{
		Term:         5,
		CandidateID:  "b",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.VoteGranted {
		t.Fatal("node granted a vote to a candidate with a stale log")
	}
}

func waitForLeader(t *testing.T, nodes []*Node, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := -1
		leaders := 0
		for index, node := range nodes {
			if node != nil && node.Status().Role == Leader {
				leader = index
				leaders++
			}
		}
		if leaders == 1 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("cluster did not converge on one leader")
	return -1
}

func firstAliveExcept(nodes []*Node, excluded int) int {
	for index, node := range nodes {
		if node != nil && index != excluded {
			return index
		}
	}
	return -1
}

func waitForLeaderKnowledge(t *testing.T, nodes []*Node, leader int, timeout time.Duration) {
	t.Helper()
	want := nodes[leader].Status().ID
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		converged := true
		for _, node := range nodes {
			if node == nil {
				continue
			}
			status := node.Status()
			if status.LeaderID != want {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("followers did not learn leader %q", want)
}

func waitForAppliedValue(t *testing.T, nodes []*Node, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		matches := 0
		alive := 0
		for _, node := range nodes {
			if node == nil {
				continue
			}
			alive++
			if value, ok := node.Get(key); ok && value == want {
				matches++
			}
		}
		if matches == alive {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("value %q did not reach every live node", key)
}

func putValue(t *testing.T, baseURL, key, value string, wantStatus int) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"value": value})
	request, _ := http.NewRequest(http.MethodPut, baseURL+"/v1/kv/"+key, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("PUT status = %d, body = %s; want %d", response.StatusCode, data, wantStatus)
	}
}

func getValue(t *testing.T, baseURL, key string, wantStatus int) string {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(baseURL + "/v1/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("GET status = %d, body = %s; want %d", response.StatusCode, data, wantStatus)
	}
	if wantStatus != http.StatusOK {
		return ""
	}
	var result struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result.Value
}
