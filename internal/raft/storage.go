package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var ErrStateNotFound = errors.New("raft state not found")

type Storage interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

// FileStorage persists the complete Raft metadata and log with atomic rename.
// Rewriting the log is intentionally simple for this educational baseline; a
// segmented WAL is the next storage optimization.
type FileStorage struct {
	path string
}

func NewFileStorage(dataDirectory string) (*FileStorage, error) {
	if dataDirectory == "" {
		return nil, errors.New("data directory is required")
	}
	if err := os.MkdirAll(dataDirectory, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	return &FileStorage{path: filepath.Join(dataDirectory, "raft-state.json")}, nil
}

func (s *FileStorage) Load() (PersistentState, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, ErrStateNotFound
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("read Raft state: %w", err)
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("decode Raft state: %w", err)
	}
	return state, nil
}

func (s *FileStorage) Save(state PersistentState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Raft state: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".raft-state-*")
	if err != nil {
		return fmt.Errorf("create temporary Raft state: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	if _, err := temporary.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write Raft state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync Raft state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("close Raft state: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("replace Raft state: %w", err)
	}
	return nil
}

type MemoryStorage struct {
	mu      sync.Mutex
	state   PersistentState
	present bool
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.present {
		return PersistentState{}, ErrStateNotFound
	}
	return clonePersistentState(s.state), nil
}

func (s *MemoryStorage) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = clonePersistentState(state)
	s.present = true
	return nil
}

func clonePersistentState(state PersistentState) PersistentState {
	cloned := state
	cloned.Log = append([]Entry(nil), state.Log...)
	return cloned
}
