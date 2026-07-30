// Package ring maps keys to replicas using rendezvous (highest-random-weight)
// hashing. Unlike a traditional hash ring, membership changes require no
// virtual nodes and remap only keys affected by the changed member.
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

type Member struct {
	ID  string
	URL string
}

type weightedMember struct {
	member Member
	weight uint64
}

// Select returns up to count distinct members in deterministic preference
// order for key.
func Select(key string, members []Member, count int) []Member {
	if count <= 0 || len(members) == 0 {
		return nil
	}

	weighted := make([]weightedMember, 0, len(members))
	for _, member := range members {
		sum := sha256.Sum256([]byte(key + "\x00" + member.ID))
		weighted = append(weighted, weightedMember{
			member: member,
			weight: binary.BigEndian.Uint64(sum[:8]),
		})
	}
	sort.Slice(weighted, func(i, j int) bool {
		if weighted[i].weight == weighted[j].weight {
			return weighted[i].member.ID < weighted[j].member.ID
		}
		return weighted[i].weight > weighted[j].weight
	})

	if count > len(weighted) {
		count = len(weighted)
	}
	selected := make([]Member, count)
	for i := range count {
		selected[i] = weighted[i].member
	}
	return selected
}
