// Package store provides concurrency-safe, version-aware local storage.
package store

import (
	"sync"

	"github.com/jiholee5217/distributed-kv-store/internal/kv"
)

// Store is an in-memory last-write-wins key-value store.
type Store struct {
	mu      sync.RWMutex
	records map[string]kv.Record
}

func New() *Store {
	return &Store{records: make(map[string]kv.Record)}
}

// Apply stores record only when it is newer than the current version. It
// returns true when local state changed.
func (s *Store) Apply(key string, record kv.Record) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.records[key]
	if ok && record.Version.Compare(current.Version) <= 0 {
		return false
	}
	s.records[key] = record
	return true
}

func (s *Store) Get(key string) (kv.Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.records[key]
	return record, ok
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}
