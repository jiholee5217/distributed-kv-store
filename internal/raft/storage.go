package raft

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrStateNotFound = errors.New("raft state not found")
	ErrStateCorrupt  = errors.New("raft state is corrupt")
)

const fileStorageVersion = 1

type fileStorageEnvelope struct {
	Version  int             `json:"version"`
	Checksum string          `json:"checksum"`
	State    PersistentState `json:"state"`
}

type Storage interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

// FileStorage persists the complete Raft metadata and log in a versioned,
// checksummed envelope. Rewriting the log is intentionally simple for this
// educational baseline; a segmented WAL is the next storage optimization.
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
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return PersistentState{}, fmt.Errorf("%w: decode state file: %v", ErrStateCorrupt, err)
	}

	_, hasVersion := fields["version"]
	_, hasChecksum := fields["checksum"]
	_, hasState := fields["state"]
	if !hasVersion && !hasChecksum && !hasState {
		return decodeLegacyState(data)
	}
	if !hasVersion || !hasChecksum || !hasState {
		return PersistentState{}, fmt.Errorf("%w: incomplete storage envelope", ErrStateCorrupt)
	}

	var envelope fileStorageEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return PersistentState{}, fmt.Errorf("%w: decode storage envelope: %v", ErrStateCorrupt, err)
	}
	if envelope.Version != fileStorageVersion {
		return PersistentState{}, fmt.Errorf("%w: unsupported storage version %d", ErrStateCorrupt, envelope.Version)
	}
	checksum, err := stateChecksum(envelope.State)
	if err != nil {
		return PersistentState{}, fmt.Errorf("%w: calculate checksum: %v", ErrStateCorrupt, err)
	}
	if !strings.EqualFold(envelope.Checksum, checksum) {
		return PersistentState{}, fmt.Errorf("%w: checksum mismatch", ErrStateCorrupt)
	}
	return envelope.State, nil
}

func (s *FileStorage) Save(state PersistentState) error {
	checksum, err := stateChecksum(state)
	if err != nil {
		return fmt.Errorf("calculate Raft state checksum: %w", err)
	}
	data, err := json.MarshalIndent(fileStorageEnvelope{
		Version:  fileStorageVersion,
		Checksum: checksum,
		State:    state,
	}, "", "  ")
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
	directory, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("open Raft state directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync Raft state directory: %w", err)
	}
	return nil
}

func stateChecksum(state PersistentState) (string, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func decodeLegacyState(data []byte) (PersistentState, error) {
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("%w: decode legacy state: %v", ErrStateCorrupt, err)
	}
	return state, nil
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
