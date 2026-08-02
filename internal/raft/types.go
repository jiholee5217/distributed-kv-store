package raft

import "github.com/jiholee5217/distributed-kv-store/internal/statemachine"

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Member struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type Entry struct {
	Index   uint64               `json:"index"`
	Term    uint64               `json:"term"`
	Command statemachine.Command `json:"command"`
}

type PersistentState struct {
	CurrentTerm uint64  `json:"current_term"`
	VotedFor    string  `json:"voted_for,omitempty"`
	Log         []Entry `json:"log"`
	CommitIndex uint64  `json:"commit_index"`
}

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	MatchIndex    uint64 `json:"match_index"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

type Status struct {
	ID           string `json:"id"`
	Role         Role   `json:"role"`
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leader_id,omitempty"`
	CommitIndex  uint64 `json:"commit_index"`
	LastApplied  uint64 `json:"last_applied"`
	LastLogIndex uint64 `json:"last_log_index"`
	Members      int    `json:"members"`
	LastError    string `json:"last_error,omitempty"`
}
