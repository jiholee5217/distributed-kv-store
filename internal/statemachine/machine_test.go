package statemachine

import "testing"

func TestMachineAppliesCommandsInOrder(t *testing.T) {
	machine := New()
	if err := machine.Apply(Command{Operation: OperationPut, Key: "language", Value: "Go"}); err != nil {
		t.Fatal(err)
	}
	if got, ok := machine.Get("language"); !ok || got != "Go" {
		t.Fatalf("Get() = %q, %v; want Go, true", got, ok)
	}
	if err := machine.Apply(Command{Operation: OperationDelete, Key: "language"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := machine.Get("language"); ok {
		t.Fatal("deleted key remains present")
	}
	if err := machine.Apply(Command{Operation: OperationBarrier}); err != nil {
		t.Fatalf("barrier failed: %v", err)
	}
}
