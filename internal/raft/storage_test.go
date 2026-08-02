package raft

import (
	"path/filepath"
	"testing"

	"github.com/jiholee5217/distributed-kv-store/internal/statemachine"
)

func TestFileStorageRoundTrip(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "node-1")
	storage, err := NewFileStorage(directory)
	if err != nil {
		t.Fatal(err)
	}
	want := PersistentState{
		CurrentTerm: 4,
		VotedFor:    "node-2",
		CommitIndex: 1,
		Log: []Entry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 4, Command: statemachine.Command{Operation: statemachine.OperationPut, Key: "k", Value: "v"}},
		},
	}
	if err := storage.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentTerm != want.CurrentTerm || got.CommitIndex != want.CommitIndex || len(got.Log) != 2 {
		t.Fatalf("Load() = %#v; want %#v", got, want)
	}
}
