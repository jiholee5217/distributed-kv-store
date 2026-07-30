package ring

import (
	"reflect"
	"testing"
)

func TestSelectIsDeterministicAndBounded(t *testing.T) {
	members := []Member{
		{ID: "a", URL: "http://a"},
		{ID: "b", URL: "http://b"},
		{ID: "c", URL: "http://c"},
	}

	first := Select("customer:42", members, 2)
	second := Select("customer:42", members, 2)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Select() changed between calls: %#v != %#v", first, second)
	}
	if len(first) != 2 || first[0].ID == first[1].ID {
		t.Fatalf("Select() = %#v; want two distinct members", first)
	}
}

func TestSelectCapsReplicationAtMembershipSize(t *testing.T) {
	members := []Member{{ID: "a"}, {ID: "b"}}
	if got := len(Select("key", members, 10)); got != 2 {
		t.Fatalf("len(Select()) = %d; want 2", got)
	}
}
