package raft

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
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

	data, err := os.ReadFile(filepath.Join(directory, "raft-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var envelope fileStorageEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Version != fileStorageVersion || len(envelope.Checksum) != sha256HexLength {
		t.Fatalf("unexpected storage envelope: %#v", envelope)
	}
}

const sha256HexLength = 64

func TestFileStorageRejectsChecksumMismatch(t *testing.T) {
	directory := t.TempDir()
	storage, err := NewFileStorage(directory)
	if err != nil {
		t.Fatal(err)
	}
	state := PersistentState{
		CurrentTerm: 2,
		Log:         []Entry{{Index: 0, Term: 0}},
	}
	if err := storage.Save(state); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(directory, "raft-state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := strings.Replace(string(data), `"current_term": 2`, `"current_term": 9`, 1)
	if corrupted == string(data) {
		t.Fatal("test failed to modify persisted state")
	}
	if err := os.WriteFile(path, []byte(corrupted), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := storage.Load(); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("Load() error = %v; want ErrStateCorrupt", err)
	}
}

func TestFileStorageLoadsLegacyState(t *testing.T) {
	directory := t.TempDir()
	storage, err := NewFileStorage(directory)
	if err != nil {
		t.Fatal(err)
	}
	want := PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-4",
		CommitIndex: 0,
		Log:         []Entry{{Index: 0, Term: 0}},
	}
	data, err := json.MarshalIndent(want, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "raft-state.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentTerm != want.CurrentTerm || got.VotedFor != want.VotedFor || len(got.Log) != 1 {
		t.Fatalf("Load() = %#v; want %#v", got, want)
	}
}

func TestFileStorageRejectsIncompleteEnvelope(t *testing.T) {
	directory := t.TempDir()
	storage, err := NewFileStorage(directory)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"state":{"current_term":1,"log":[],"commit_index":0}}`)
	if err := os.WriteFile(filepath.Join(directory, "raft-state.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := storage.Load(); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("Load() error = %v; want ErrStateCorrupt", err)
	}
}
