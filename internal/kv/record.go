// Package kv defines the values and versions shared by storage and transport.
package kv

// Version totally orders writes. WallTime is generated from a monotonic local
// clock and NodeID deterministically breaks ties between coordinators.
type Version struct {
	WallTime int64  `json:"wall_time"`
	NodeID   string `json:"node_id"`
}

// Compare returns -1, 0, or 1 when v is older than, equal to, or newer than
// other.
func (v Version) Compare(other Version) int {
	switch {
	case v.WallTime < other.WallTime:
		return -1
	case v.WallTime > other.WallTime:
		return 1
	case v.NodeID < other.NodeID:
		return -1
	case v.NodeID > other.NodeID:
		return 1
	default:
		return 0
	}
}

// Record is the unit replicated between nodes. Deletes are retained as
// tombstones so an older value cannot be resurrected by a delayed replica.
type Record struct {
	Value     string  `json:"value,omitempty"`
	Version   Version `json:"version"`
	Tombstone bool    `json:"tombstone,omitempty"`
}
