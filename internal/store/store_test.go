package store

import (
	"testing"

	"github.com/jiholee5217/distributed-kv-store/internal/kv"
)

func TestApplyKeepsNewestVersion(t *testing.T) {
	s := New()
	newer := kv.Record{Value: "new", Version: kv.Version{WallTime: 2, NodeID: "b"}}
	older := kv.Record{Value: "old", Version: kv.Version{WallTime: 1, NodeID: "a"}}

	if !s.Apply("key", newer) {
		t.Fatal("first write was not applied")
	}
	if s.Apply("key", older) {
		t.Fatal("stale write replaced the newest value")
	}

	got, ok := s.Get("key")
	if !ok || got.Value != "new" {
		t.Fatalf("Get() = %#v, %v; want newest value", got, ok)
	}
}

func TestApplyUsesNodeIDToBreakTies(t *testing.T) {
	s := New()
	s.Apply("key", kv.Record{Value: "a", Version: kv.Version{WallTime: 1, NodeID: "a"}})
	s.Apply("key", kv.Record{Value: "b", Version: kv.Version{WallTime: 1, NodeID: "b"}})

	got, _ := s.Get("key")
	if got.Value != "b" {
		t.Fatalf("Get().Value = %q; want b", got.Value)
	}
}

func TestTombstonePreventsResurrection(t *testing.T) {
	s := New()
	s.Apply("key", kv.Record{Tombstone: true, Version: kv.Version{WallTime: 2, NodeID: "a"}})

	if s.Apply("key", kv.Record{Value: "stale", Version: kv.Version{WallTime: 1, NodeID: "b"}}) {
		t.Fatal("stale value replaced a newer tombstone")
	}
	got, _ := s.Get("key")
	if !got.Tombstone {
		t.Fatal("tombstone was lost")
	}
}
