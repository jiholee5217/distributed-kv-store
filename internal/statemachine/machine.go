// Package statemachine implements the deterministic key-value state machine
// driven by committed Raft log entries.
package statemachine

import (
	"errors"
	"sync"
)

type Operation string

const (
	OperationPut     Operation = "put"
	OperationDelete  Operation = "delete"
	OperationBarrier Operation = "barrier"
)

// Command is the replicated operation stored in the Raft log.
type Command struct {
	Operation Operation `json:"operation"`
	Key       string    `json:"key,omitempty"`
	Value     string    `json:"value,omitempty"`
}

// Machine stores values produced by applying committed commands in log order.
type Machine struct {
	mu     sync.RWMutex
	values map[string]string
}

func New() *Machine {
	return &Machine{values: make(map[string]string)}
}

func (m *Machine) Apply(command Command) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch command.Operation {
	case OperationPut:
		if command.Key == "" {
			return errors.New("put command requires a key")
		}
		m.values[command.Key] = command.Value
	case OperationDelete:
		if command.Key == "" {
			return errors.New("delete command requires a key")
		}
		delete(m.values, command.Key)
	case OperationBarrier:
		// A barrier intentionally changes no data. Once committed, it proves the
		// leader contacted a majority in its current term before serving a read.
	default:
		return errors.New("unknown state-machine operation")
	}
	return nil
}

func (m *Machine) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.values[key]
	return value, ok
}

func (m *Machine) Snapshot() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]string, len(m.values))
	for key, value := range m.values {
		result[key] = value
	}
	return result
}
