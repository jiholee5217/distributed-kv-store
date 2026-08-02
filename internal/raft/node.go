package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/jiholee5217/distributed-kv-store/internal/statemachine"
)

var (
	ErrNotLeader = errors.New("node is not the Raft leader")
	ErrStopped   = errors.New("Raft node is stopped")
)

type Config struct {
	ID                 string
	Members            []Member
	Storage            Storage
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RPCTimeout         time.Duration
	ForwardTimeout     time.Duration
}

// Node owns the Raft consensus state and deterministic key-value state
// machine. All fields below mu are accessed while mu is held unless noted.
type Node struct {
	mu sync.Mutex

	id            string
	members       []Member
	byID          map[string]Member
	storage       Storage
	machine       *statemachine.Machine
	rpcClient     *http.Client
	forwardClient *http.Client

	role          Role
	currentTerm   uint64
	votedFor      string
	log           []Entry
	commitIndex   uint64
	lastApplied   uint64
	leaderID      string
	nextIndex     map[string]uint64
	matchIndex    map[string]uint64
	lastError     string
	electionDue   time.Time
	lastHeartbeat time.Time
	replicating   bool
	started       bool

	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewNode(config Config) (*Node, error) {
	if config.ID == "" {
		return nil, errors.New("node ID is required")
	}
	if config.Storage == nil {
		return nil, errors.New("Raft storage is required")
	}
	if len(config.Members) == 0 {
		return nil, errors.New("at least one cluster member is required")
	}

	byID := make(map[string]Member, len(config.Members))
	for _, member := range config.Members {
		if member.ID == "" || member.URL == "" {
			return nil, errors.New("every member requires an ID and URL")
		}
		if _, exists := byID[member.ID]; exists {
			return nil, fmt.Errorf("duplicate member ID %q", member.ID)
		}
		byID[member.ID] = member
	}
	if _, exists := byID[config.ID]; !exists {
		return nil, fmt.Errorf("node %q is missing from cluster membership", config.ID)
	}

	if config.ElectionTimeoutMin <= 0 {
		config.ElectionTimeoutMin = 350 * time.Millisecond
	}
	if config.ElectionTimeoutMax <= config.ElectionTimeoutMin {
		config.ElectionTimeoutMax = 700 * time.Millisecond
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 100 * time.Millisecond
	}
	if config.HeartbeatInterval >= config.ElectionTimeoutMin {
		return nil, errors.New("heartbeat interval must be shorter than the minimum election timeout")
	}
	if config.RPCTimeout <= 0 {
		config.RPCTimeout = 250 * time.Millisecond
	}
	if config.ForwardTimeout <= 0 {
		config.ForwardTimeout = 10 * time.Second
	}

	persistent, err := config.Storage.Load()
	if errors.Is(err, ErrStateNotFound) {
		persistent = PersistentState{Log: []Entry{{Index: 0, Term: 0}}}
		if err := config.Storage.Save(persistent); err != nil {
			return nil, fmt.Errorf("initialize Raft storage: %w", err)
		}
	} else if err != nil {
		return nil, err
	}
	if err := validatePersistentState(persistent); err != nil {
		return nil, err
	}

	machine := statemachine.New()
	for index := uint64(1); index <= persistent.CommitIndex; index++ {
		if err := machine.Apply(persistent.Log[index].Command); err != nil {
			return nil, fmt.Errorf("replay committed entry %d: %w", index, err)
		}
	}

	node := &Node{
		id:            config.ID,
		members:       append([]Member(nil), config.Members...),
		byID:          byID,
		storage:       config.Storage,
		machine:       machine,
		rpcClient:     &http.Client{Timeout: config.RPCTimeout},
		forwardClient: &http.Client{Timeout: config.ForwardTimeout},
		role:          Follower,
		currentTerm:   persistent.CurrentTerm,
		votedFor:      persistent.VotedFor,
		log:           append([]Entry(nil), persistent.Log...),
		commitIndex:   persistent.CommitIndex,
		lastApplied:   persistent.CommitIndex,
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		electionMin:   config.ElectionTimeoutMin,
		electionMax:   config.ElectionTimeoutMax,
		heartbeat:     config.HeartbeatInterval,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	node.resetElectionDeadlineLocked()
	return node, nil
}

func validatePersistentState(state PersistentState) error {
	if len(state.Log) == 0 || state.Log[0].Index != 0 || state.Log[0].Term != 0 {
		return errors.New("persisted Raft log is missing its sentinel entry")
	}
	for index, entry := range state.Log {
		if entry.Index != uint64(index) {
			return fmt.Errorf("persisted log entry %d has index %d", index, entry.Index)
		}
	}
	if state.CommitIndex >= uint64(len(state.Log)) {
		return errors.New("persisted commit index exceeds the log")
	}
	return nil
}

func (n *Node) Start() {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return
	}
	n.started = true
	n.resetElectionDeadlineLocked()
	n.mu.Unlock()
	go n.run()
}

func (n *Node) Close() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	n.mu.Lock()
	started := n.started
	n.mu.Unlock()
	if started {
		<-n.doneCh
	}
}

func (n *Node) run() {
	defer close(n.doneCh)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case now := <-ticker.C:
			n.mu.Lock()
			role := n.role
			startElection := role != Leader && !now.Before(n.electionDue)
			heartbeat := role == Leader && now.Sub(n.lastHeartbeat) >= n.heartbeat
			if heartbeat {
				n.lastHeartbeat = now
			}
			n.mu.Unlock()

			if startElection {
				go n.startElection()
			}
			if heartbeat {
				n.requestReplication()
			}
		}
	}
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:           n.id,
		Role:         n.role,
		Term:         n.currentTerm,
		LeaderID:     n.leaderID,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: n.lastLogIndexLocked(),
		Members:      len(n.members),
		LastError:    n.lastError,
	}
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

func (n *Node) Get(key string) (string, bool) { return n.machine.Get(key) }

func (n *Node) Propose(ctx context.Context, command statemachine.Command) (uint64, uint64, error) {
	select {
	case <-n.stopCh:
		return 0, 0, ErrStopped
	default:
	}

	n.mu.Lock()
	if n.role != Leader {
		term := n.currentTerm
		n.mu.Unlock()
		return 0, term, ErrNotLeader
	}
	entry := Entry{
		Index:   uint64(len(n.log)),
		Term:    n.currentTerm,
		Command: command,
	}
	n.log = append(n.log, entry)
	n.matchIndex[n.id] = entry.Index
	if err := n.persistLocked(); err != nil {
		n.log = n.log[:len(n.log)-1]
		n.lastError = err.Error()
		term := n.currentTerm
		n.mu.Unlock()
		return 0, term, err
	}
	if len(n.members) == 1 {
		n.advanceCommitLocked()
	}
	term := n.currentTerm
	n.mu.Unlock()

	n.requestReplication()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return entry.Index, term, ctx.Err()
		case <-n.stopCh:
			return entry.Index, term, ErrStopped
		case <-ticker.C:
			n.mu.Lock()
			committed := n.commitIndex >= entry.Index
			stillLeader := n.role == Leader && n.currentTerm == term
			n.mu.Unlock()
			if committed {
				return entry.Index, term, nil
			}
			if !stillLeader {
				return entry.Index, term, ErrNotLeader
			}
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == Leader || time.Now().Before(n.electionDue) {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionDeadlineLocked()
	term := n.currentTerm
	lastIndex := n.lastLogIndexLocked()
	lastTerm := n.log[lastIndex].Term
	if err := n.persistLocked(); err != nil {
		n.lastError = err.Error()
		n.role = Follower
		n.mu.Unlock()
		return
	}
	majority := n.majority()
	if majority == 1 {
		becameLeader := n.becomeLeaderLocked()
		n.mu.Unlock()
		if becameLeader {
			n.requestReplication()
		}
		return
	}
	n.mu.Unlock()

	type result struct {
		response RequestVoteResponse
		err      error
	}
	results := make(chan result, len(n.members)-1)
	request := RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	for _, member := range n.members {
		if member.ID == n.id {
			continue
		}
		member := member
		go func() {
			response, err := n.sendRequestVote(member, request)
			results <- result{response: response, err: err}
		}()
	}

	votes := 1
	for range len(n.members) - 1 {
		result := <-results
		if result.err != nil {
			continue
		}
		n.mu.Lock()
		if result.response.Term > n.currentTerm {
			n.becomeFollowerLocked(result.response.Term, "")
			_ = n.persistLocked()
			n.mu.Unlock()
			return
		}
		if n.role != Candidate || n.currentTerm != term {
			n.mu.Unlock()
			continue
		}
		if result.response.VoteGranted {
			votes++
		}
		if votes >= majority {
			becameLeader := n.becomeLeaderLocked()
			n.mu.Unlock()
			if becameLeader {
				n.requestReplication()
			}
			return
		}
		n.mu.Unlock()
	}
}

func (n *Node) becomeLeaderLocked() bool {
	if n.role != Candidate {
		return false
	}
	n.role = Leader
	n.leaderID = n.id
	n.lastHeartbeat = time.Time{}
	n.nextIndex = make(map[string]uint64, len(n.members))
	n.matchIndex = make(map[string]uint64, len(n.members))
	next := uint64(len(n.log))
	for _, member := range n.members {
		n.nextIndex[member.ID] = next
		n.matchIndex[member.ID] = 0
	}

	// A new leader appends a no-op barrier in its own term. Committing it also
	// commits any safe entries inherited from previous terms.
	entry := Entry{
		Index:   next,
		Term:    n.currentTerm,
		Command: statemachine.Command{Operation: statemachine.OperationBarrier},
	}
	n.log = append(n.log, entry)
	n.matchIndex[n.id] = entry.Index
	n.nextIndex[n.id] = entry.Index + 1
	if err := n.persistLocked(); err != nil {
		n.log = n.log[:len(n.log)-1]
		n.role = Follower
		n.leaderID = ""
		n.lastError = err.Error()
		return false
	}
	if len(n.members) == 1 {
		n.advanceCommitLocked()
	}
	slog.Info("Raft leader elected", "node", n.id, "term", n.currentTerm)
	return true
}

func (n *Node) becomeFollowerLocked(term uint64, leaderID string) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.role = Follower
	n.leaderID = leaderID
	n.replicating = false
	n.resetElectionDeadlineLocked()
}

func (n *Node) HandleRequestVote(request RequestVoteRequest) (RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if request.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm}, nil
	}
	changed := false
	if request.Term > n.currentTerm {
		n.becomeFollowerLocked(request.Term, "")
		changed = true
	}

	lastIndex := n.lastLogIndexLocked()
	lastTerm := n.log[lastIndex].Term
	upToDate := request.LastLogTerm > lastTerm ||
		(request.LastLogTerm == lastTerm && request.LastLogIndex >= lastIndex)
	grant := upToDate && (n.votedFor == "" || n.votedFor == request.CandidateID)
	if grant {
		n.votedFor = request.CandidateID
		n.resetElectionDeadlineLocked()
		changed = true
	}
	if changed {
		if err := n.persistLocked(); err != nil {
			n.lastError = err.Error()
			return RequestVoteResponse{Term: n.currentTerm}, err
		}
	}
	return RequestVoteResponse{Term: n.currentTerm, VoteGranted: grant}, nil
}

func (n *Node) HandleAppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if request.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm}, nil
	}
	if request.Term > n.currentTerm || n.role != Follower || n.leaderID != request.LeaderID {
		n.becomeFollowerLocked(request.Term, request.LeaderID)
	} else {
		n.resetElectionDeadlineLocked()
	}

	if request.PrevLogIndex >= uint64(len(n.log)) {
		if err := n.persistLocked(); err != nil {
			return AppendEntriesResponse{Term: n.currentTerm}, err
		}
		return AppendEntriesResponse{
			Term:          n.currentTerm,
			ConflictIndex: uint64(len(n.log)),
		}, nil
	}
	if n.log[request.PrevLogIndex].Term != request.PrevLogTerm {
		conflictTerm := n.log[request.PrevLogIndex].Term
		conflictIndex := request.PrevLogIndex
		for conflictIndex > 1 && n.log[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		if err := n.persistLocked(); err != nil {
			return AppendEntriesResponse{Term: n.currentTerm}, err
		}
		return AppendEntriesResponse{Term: n.currentTerm, ConflictIndex: conflictIndex}, nil
	}

	insertAt := request.PrevLogIndex + 1
	for offset, incoming := range request.Entries {
		index := insertAt + uint64(offset)
		if index < uint64(len(n.log)) {
			if n.log[index].Term != incoming.Term {
				n.log = append([]Entry(nil), n.log[:index]...)
				n.log = append(n.log, request.Entries[offset:]...)
				break
			}
			continue
		}
		n.log = append(n.log, request.Entries[offset:]...)
		break
	}

	if request.LeaderCommit > n.commitIndex {
		n.commitIndex = min(request.LeaderCommit, n.lastLogIndexLocked())
	}
	if err := n.persistLocked(); err != nil {
		n.lastError = err.Error()
		return AppendEntriesResponse{Term: n.currentTerm}, err
	}
	n.applyCommittedLocked()
	matchIndex := request.PrevLogIndex + uint64(len(request.Entries))
	return AppendEntriesResponse{
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: matchIndex,
	}, nil
}

func (n *Node) requestReplication() {
	n.mu.Lock()
	if n.role != Leader || n.replicating {
		n.mu.Unlock()
		return
	}
	n.replicating = true
	n.mu.Unlock()
	go n.replicateAll()
}

func (n *Node) replicateAll() {
	var wait sync.WaitGroup
	for _, member := range n.members {
		if member.ID == n.id {
			continue
		}
		wait.Add(1)
		member := member
		go func() {
			defer wait.Done()
			n.replicatePeer(member)
		}()
	}
	wait.Wait()
	n.mu.Lock()
	n.replicating = false
	n.mu.Unlock()
}

func (n *Node) replicatePeer(member Member) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	next := n.nextIndex[member.ID]
	if next == 0 {
		next = 1
	}
	if next > uint64(len(n.log)) {
		next = uint64(len(n.log))
	}
	previous := next - 1
	entries := append([]Entry(nil), n.log[next:]...)
	request := AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: previous,
		PrevLogTerm:  n.log[previous].Term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	response, err := n.sendAppendEntries(member, request)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if response.Term > n.currentTerm {
		n.becomeFollowerLocked(response.Term, "")
		_ = n.persistLocked()
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}
	if response.Success {
		n.matchIndex[member.ID] = response.MatchIndex
		n.nextIndex[member.ID] = response.MatchIndex + 1
		n.advanceCommitLocked()
		return
	}
	if response.ConflictIndex > 0 {
		n.nextIndex[member.ID] = response.ConflictIndex
	} else if n.nextIndex[member.ID] > 1 {
		n.nextIndex[member.ID]--
	}
}

func (n *Node) advanceCommitLocked() {
	for candidate := n.lastLogIndexLocked(); candidate > n.commitIndex; candidate-- {
		if n.log[candidate].Term != n.currentTerm {
			continue
		}
		votes := 0
		for _, member := range n.members {
			if member.ID == n.id || n.matchIndex[member.ID] >= candidate {
				votes++
			}
		}
		if votes < n.majority() {
			continue
		}
		previous := n.commitIndex
		n.commitIndex = candidate
		if err := n.persistLocked(); err != nil {
			n.commitIndex = previous
			n.lastError = err.Error()
			return
		}
		n.applyCommittedLocked()
		return
	}
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if err := n.machine.Apply(n.log[n.lastApplied].Command); err != nil {
			n.lastError = fmt.Sprintf("apply entry %d: %v", n.lastApplied, err)
			return
		}
	}
}

func (n *Node) sendRequestVote(member Member, request RequestVoteRequest) (RequestVoteResponse, error) {
	var response RequestVoteResponse
	err := n.postJSON(member.URL+"/internal/raft/request-vote", request, &response)
	return response, err
}

func (n *Node) sendAppendEntries(member Member, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	var response AppendEntriesResponse
	err := n.postJSON(member.URL+"/internal/raft/append-entries", request, &response)
	return response, err
}

func (n *Node) postJSON(endpoint string, payload, destination any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := n.rpcClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		io.Copy(io.Discard, response.Body)
		return fmt.Errorf("Raft peer returned %s", response.Status)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(destination); err != nil {
		return err
	}
	return nil
}

func (n *Node) persistLocked() error {
	return n.storage.Save(PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         append([]Entry(nil), n.log...),
		CommitIndex: n.commitIndex,
	})
}

func (n *Node) majority() int { return len(n.members)/2 + 1 }

func (n *Node) lastLogIndexLocked() uint64 { return uint64(len(n.log) - 1) }

func (n *Node) resetElectionDeadlineLocked() {
	spread := n.electionMax - n.electionMin
	delay := n.electionMin
	if spread > 0 {
		delay += time.Duration(rand.Int64N(int64(spread)))
	}
	n.electionDue = time.Now().Add(delay)
}

func (n *Node) leaderLocation() (string, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	member, ok := n.byID[n.leaderID]
	if !ok {
		return n.leaderID, ""
	}
	return n.leaderID, member.URL
}
