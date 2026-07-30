// Package node implements the public and replica-facing HTTP APIs.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jiholee5217/distributed-kv-store/internal/kv"
	"github.com/jiholee5217/distributed-kv-store/internal/ring"
	"github.com/jiholee5217/distributed-kv-store/internal/store"
)

const maxRequestBody = 1 << 20

type Config struct {
	ID                string
	Members           []ring.Member
	ReplicationFactor int
	ReadQuorum        int
	WriteQuorum       int
	RequestTimeout    time.Duration
}

type Node struct {
	id                string
	members           []ring.Member
	replicationFactor int
	readQuorum        int
	writeQuorum       int
	store             *store.Store
	client            *http.Client
	clock             atomic.Int64
}

func New(config Config) (*Node, error) {
	if config.ID == "" {
		return nil, errors.New("node ID is required")
	}
	if len(config.Members) == 0 {
		return nil, errors.New("at least one member is required")
	}

	seen := make(map[string]struct{}, len(config.Members))
	selfFound := false
	for _, member := range config.Members {
		if member.ID == "" || member.URL == "" {
			return nil, errors.New("every member requires an ID and URL")
		}
		if _, ok := seen[member.ID]; ok {
			return nil, fmt.Errorf("duplicate member ID %q", member.ID)
		}
		seen[member.ID] = struct{}{}
		if member.ID == config.ID {
			selfFound = true
		}
	}
	if !selfFound {
		return nil, fmt.Errorf("node %q is missing from membership", config.ID)
	}

	replicas := config.ReplicationFactor
	if replicas <= 0 {
		return nil, errors.New("replication factor must be positive")
	}
	if replicas > len(config.Members) {
		replicas = len(config.Members)
	}
	if config.ReadQuorum <= 0 || config.ReadQuorum > replicas {
		return nil, fmt.Errorf("read quorum must be between 1 and %d", replicas)
	}
	if config.WriteQuorum <= 0 || config.WriteQuorum > replicas {
		return nil, fmt.Errorf("write quorum must be between 1 and %d", replicas)
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 1500 * time.Millisecond
	}

	return &Node{
		id:                config.ID,
		members:           append([]ring.Member(nil), config.Members...),
		replicationFactor: replicas,
		readQuorum:        config.ReadQuorum,
		writeQuorum:       config.WriteQuorum,
		store:             store.New(),
		client:            &http.Client{Timeout: config.RequestTimeout},
	}, nil
}

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", n.handleHealth)
	mux.HandleFunc("GET /v1/kv/{key}", n.handleGet)
	mux.HandleFunc("PUT /v1/kv/{key}", n.handlePut)
	mux.HandleFunc("DELETE /v1/kv/{key}", n.handleDelete)
	mux.HandleFunc("GET /internal/v1/records/{key}", n.handleReplicaGet)
	mux.HandleFunc("PUT /internal/v1/records/{key}", n.handleReplicaPut)
	return requestLogger(mux)
}

func (n *Node) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":             "ok",
		"node_id":            n.id,
		"members":            len(n.members),
		"replication_factor": n.replicationFactor,
	})
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := validateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	record, found, err := n.read(r.Context(), key)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if !found || record.Tombstone {
		writeError(w, http.StatusNotFound, errors.New("key not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"key":     key,
		"value":   record.Value,
		"version": record.Version,
	})
}

type putRequest struct {
	Value string `json:"value"`
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := validateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var input putRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	record := kv.Record{Value: input.Value, Version: n.nextVersion()}
	acks, err := n.write(r.Context(), key, record)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":        err.Error(),
			"acknowledged": acks,
			"write_quorum": n.writeQuorum,
		})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"key":          key,
		"version":      record.Version,
		"acknowledged": acks,
	})
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := validateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	record := kv.Record{Version: n.nextVersion(), Tombstone: true}
	acks, err := n.write(r.Context(), key, record)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":        err.Error(),
			"acknowledged": acks,
			"write_quorum": n.writeQuorum,
		})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleReplicaGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	record, ok := n.store.Get(key)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (n *Node) handleReplicaPut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := validateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var record kv.Record
	if err := decodeJSON(w, r, &record); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if record.Version.NodeID == "" || record.Version.WallTime <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid record version"))
		return
	}
	n.observe(record.Version.WallTime)
	n.store.Apply(key, record)
	w.WriteHeader(http.StatusNoContent)
}

type writeResult struct {
	err error
}

func (n *Node) write(ctx context.Context, key string, record kv.Record) (int, error) {
	replicas := ring.Select(key, n.members, n.replicationFactor)
	results := make(chan writeResult, len(replicas))
	for _, member := range replicas {
		member := member
		go func() {
			results <- writeResult{err: n.putReplica(ctx, member, key, record)}
		}()
	}

	acknowledged := 0
	for range replicas {
		if result := <-results; result.err == nil {
			acknowledged++
		}
	}
	if acknowledged < n.writeQuorum {
		return acknowledged, fmt.Errorf(
			"write quorum not reached: got %d of %d acknowledgements",
			acknowledged,
			n.writeQuorum,
		)
	}
	return acknowledged, nil
}

func (n *Node) putReplica(ctx context.Context, member ring.Member, key string, record kv.Record) error {
	if member.ID == n.id {
		n.store.Apply(key, record)
		return nil
	}

	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	endpoint := member.URL + "/internal/v1/records/" + url.PathEscape(key)
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := n.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("replica %s returned %s", member.ID, response.Status)
	}
	return nil
}

type readResult struct {
	member ring.Member
	record kv.Record
	found  bool
	err    error
}

func (n *Node) read(ctx context.Context, key string) (kv.Record, bool, error) {
	replicas := ring.Select(key, n.members, n.replicationFactor)
	results := make(chan readResult, len(replicas))
	for _, member := range replicas {
		member := member
		go func() {
			record, found, err := n.getReplica(ctx, member, key)
			results <- readResult{member: member, record: record, found: found, err: err}
		}()
	}

	successful := make([]readResult, 0, len(replicas))
	var newest kv.Record
	found := false
	for range replicas {
		result := <-results
		if result.err != nil {
			continue
		}
		successful = append(successful, result)
		if result.found && (!found || result.record.Version.Compare(newest.Version) > 0) {
			newest = result.record
			found = true
		}
	}
	if len(successful) < n.readQuorum {
		return kv.Record{}, false, fmt.Errorf(
			"read quorum not reached: got %d of %d responses",
			len(successful),
			n.readQuorum,
		)
	}
	if found {
		n.observe(newest.Version.WallTime)
		n.repair(key, newest, successful)
	}
	return newest, found, nil
}

func (n *Node) getReplica(ctx context.Context, member ring.Member, key string) (kv.Record, bool, error) {
	if member.ID == n.id {
		record, found := n.store.Get(key)
		return record, found, nil
	}

	endpoint := member.URL + "/internal/v1/records/" + url.PathEscape(key)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return kv.Record{}, false, err
	}
	response, err := n.client.Do(request)
	if err != nil {
		return kv.Record{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		io.Copy(io.Discard, response.Body)
		return kv.Record{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		io.Copy(io.Discard, response.Body)
		return kv.Record{}, false, fmt.Errorf("replica %s returned %s", member.ID, response.Status)
	}
	var record kv.Record
	if err := json.NewDecoder(io.LimitReader(response.Body, maxRequestBody)).Decode(&record); err != nil {
		return kv.Record{}, false, err
	}
	return record, true, nil
}

func (n *Node) repair(key string, newest kv.Record, results []readResult) {
	for _, result := range results {
		if result.found && result.record.Version.Compare(newest.Version) >= 0 {
			continue
		}
		member := result.member
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.client.Timeout)
			defer cancel()
			_ = n.putReplica(ctx, member, key, newest)
		}()
	}
}

func (n *Node) nextVersion() kv.Version {
	for {
		previous := n.clock.Load()
		next := time.Now().UnixNano()
		if next <= previous {
			next = previous + 1
		}
		if n.clock.CompareAndSwap(previous, next) {
			return kv.Version{WallTime: next, NodeID: n.id}
		}
	}
}

func (n *Node) observe(wallTime int64) {
	for {
		previous := n.clock.Load()
		if wallTime <= previous || n.clock.CompareAndSwap(previous, wallTime) {
			return
		}
	}
}

func validateKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("key must not be empty")
	}
	if len(key) > 512 {
		return errors.New("key must not exceed 512 bytes")
	}
	return nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		_ = start // Hook for structured logging in the observability milestone.
	})
}
