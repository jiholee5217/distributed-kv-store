package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jiholee5217/distributed-kv-store/internal/ring"
)

func TestClusterReplicatesAndToleratesOneNodeFailure(t *testing.T) {
	const size = 3
	servers := make([]*httptest.Server, size)
	for i := range servers {
		servers[i] = httptest.NewUnstartedServer(nil)
	}

	members := make([]ring.Member, size)
	for i, server := range servers {
		members[i] = ring.Member{
			ID:  fmt.Sprintf("node-%d", i+1),
			URL: "http://" + server.Listener.Addr().String(),
		}
	}
	for i, server := range servers {
		n, err := New(Config{
			ID:                members[i].ID,
			Members:           members,
			ReplicationFactor: 3,
			ReadQuorum:        2,
			WriteQuorum:       2,
			RequestTimeout:    300 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		server.Config.Handler = n.Handler()
		server.Start()
		t.Cleanup(server.Close)
	}

	put(t, servers[0].URL, "profile", "distributed-systems", http.StatusCreated)
	if got := get(t, servers[1].URL, "profile", http.StatusOK); got != "distributed-systems" {
		t.Fatalf("GET value = %q; want distributed-systems", got)
	}

	servers[2].Close()
	put(t, servers[0].URL, "profile", "fault-tolerant", http.StatusCreated)
	if got := get(t, servers[1].URL, "profile", http.StatusOK); got != "fault-tolerant" {
		t.Fatalf("GET after failure = %q; want fault-tolerant", got)
	}
}

func TestWriteFailsWithoutQuorum(t *testing.T) {
	servers := []*httptest.Server{
		httptest.NewUnstartedServer(nil),
		httptest.NewUnstartedServer(nil),
		httptest.NewUnstartedServer(nil),
	}
	members := make([]ring.Member, len(servers))
	for i, server := range servers {
		members[i] = ring.Member{
			ID:  fmt.Sprintf("node-%d", i+1),
			URL: "http://" + server.Listener.Addr().String(),
		}
	}

	n, err := New(Config{
		ID:                members[0].ID,
		Members:           members,
		ReplicationFactor: 3,
		ReadQuorum:        2,
		WriteQuorum:       2,
		RequestTimeout:    100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	servers[0].Config.Handler = n.Handler()
	servers[0].Start()
	t.Cleanup(servers[0].Close)

	put(t, servers[0].URL, "isolated", "value", http.StatusServiceUnavailable)
}

func TestDeleteUsesTombstone(t *testing.T) {
	server := httptest.NewUnstartedServer(nil)
	member := ring.Member{ID: "solo", URL: "http://" + server.Listener.Addr().String()}
	n, err := New(Config{
		ID:                "solo",
		Members:           []ring.Member{member},
		ReplicationFactor: 1,
		ReadQuorum:        1,
		WriteQuorum:       1,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.Config.Handler = n.Handler()
	server.Start()
	t.Cleanup(server.Close)

	put(t, server.URL, "temporary", "value", http.StatusCreated)
	request, _ := http.NewRequest(http.MethodDelete, server.URL+"/v1/kv/temporary", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d; want 204", response.StatusCode)
	}
	get(t, server.URL, "temporary", http.StatusNotFound)
}

func put(t *testing.T, baseURL, key, value string, wantStatus int) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"value": value})
	request, _ := http.NewRequest(http.MethodPut, baseURL+"/v1/kv/"+key, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("PUT status = %d, body = %s; want %d", response.StatusCode, body, wantStatus)
	}
}

func get(t *testing.T, baseURL, key string, wantStatus int) string {
	t.Helper()
	response, err := http.Get(baseURL + "/v1/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("GET status = %d, body = %s; want %d", response.StatusCode, body, wantStatus)
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
