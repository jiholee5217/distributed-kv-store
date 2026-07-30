package main

import "testing"

func TestParseMembers(t *testing.T) {
	members, err := parseMembers("a=http://127.0.0.1:8080,b=http://127.0.0.1:8081/")
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[1].URL != "http://127.0.0.1:8081" {
		t.Fatalf("parseMembers() = %#v", members)
	}
}

func TestParseMembersRejectsMalformedEntry(t *testing.T) {
	if _, err := parseMembers("not-a-member"); err == nil {
		t.Fatal("parseMembers() accepted a malformed entry")
	}
}
