package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/jiholee5217/distributed-kv-store/internal/statemachine"
)

const maxRequestBody = 1 << 20

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", n.handleHealth)
	mux.HandleFunc("GET /v1/status", n.handleStatus)
	mux.HandleFunc("GET /v1/kv/{key}", n.handleGet)
	mux.HandleFunc("PUT /v1/kv/{key}", n.handlePut)
	mux.HandleFunc("DELETE /v1/kv/{key}", n.handleDelete)
	mux.HandleFunc("POST /internal/raft/request-vote", n.handleRequestVote)
	mux.HandleFunc("POST /internal/raft/append-entries", n.handleAppendEntries)
	return mux
}

func (n *Node) handleHealth(w http.ResponseWriter, _ *http.Request) {
	status := n.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"node_id":   status.ID,
		"role":      status.Role,
		"term":      status.Term,
		"leader_id": status.LeaderID,
	})
}

func (n *Node) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, n.Status())
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := validateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if n.forwardToLeader(w, r) {
		return
	}

	// Committing a no-op entry is a simple read barrier. It ensures this node
	// still leads a majority before returning state-machine data.
	index, term, err := n.Propose(r.Context(), statemachine.Command{Operation: statemachine.OperationBarrier})
	if err != nil {
		writeConsensusError(w, n.Status(), err)
		return
	}
	value, found := n.Get(key)
	if !found {
		writeError(w, http.StatusNotFound, errors.New("key not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"key":         key,
		"value":       value,
		"read_index":  index,
		"leader_term": term,
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
	if n.forwardToLeader(w, r) {
		return
	}

	var input putRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	index, term, err := n.Propose(r.Context(), statemachine.Command{
		Operation: statemachine.OperationPut,
		Key:       key,
		Value:     input.Value,
	})
	if err != nil {
		writeConsensusError(w, n.Status(), err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"key":         key,
		"log_index":   index,
		"leader_term": term,
	})
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := validateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if n.forwardToLeader(w, r) {
		return
	}

	_, _, err := n.Propose(r.Context(), statemachine.Command{
		Operation: statemachine.OperationDelete,
		Key:       key,
	})
	if err != nil {
		writeConsensusError(w, n.Status(), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var request RequestVoteRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	response, err := n.HandleRequestVote(request)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (n *Node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var request AppendEntriesRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	response, err := n.HandleAppendEntries(request)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// forwardToLeader returns true when the request was proxied or rejected by a
// follower. It returns false only when this node is the leader.
func (n *Node) forwardToLeader(w http.ResponseWriter, r *http.Request) bool {
	n.mu.Lock()
	role := n.role
	leaderID := n.leaderID
	member, known := n.byID[leaderID]
	n.mu.Unlock()
	if role == Leader {
		return false
	}
	if r.Header.Get("X-Raft-Forwarded") != "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":     "forwarded request reached a non-leader",
			"leader_id": leaderID,
		})
		return true
	}
	if !known || leaderID == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "leader election in progress",
		})
		return true
	}

	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return true
		}
		if len(body) > maxRequestBody {
			writeError(w, http.StatusRequestEntityTooLarge, errors.New("request body is too large"))
			return true
		}
	}
	endpoint := strings.TrimRight(member.URL, "/") + r.URL.RequestURI()
	forwarded, err := http.NewRequestWithContext(r.Context(), r.Method, endpoint, bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return true
	}
	forwarded.Header.Set("X-Raft-Forwarded", n.id)
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		forwarded.Header.Set("Content-Type", contentType)
	}
	response, err := n.forwardClient.Do(forwarded)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":     fmt.Sprintf("leader %s is unavailable", leaderID),
			"leader_id": leaderID,
		})
		return true
	}
	defer response.Body.Close()
	if contentType := response.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.Header().Set("X-Raft-Leader", leaderID)
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(response.Body, maxRequestBody))
	return true
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

func writeConsensusError(w http.ResponseWriter, status Status, err error) {
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{
		"error":     err.Error(),
		"node_id":   status.ID,
		"role":      status.Role,
		"leader_id": status.LeaderID,
		"term":      status.Term,
	})
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
