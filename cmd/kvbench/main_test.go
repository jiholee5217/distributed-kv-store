package main

import (
	"strings"
	"testing"
	"time"
)

func TestValueWithNoncePreservesConfiguredSize(t *testing.T) {
	for _, size := range []int{0, 1, 8, 16, 64} {
		value := valueWithNonce(strings.Repeat("x", size), 42)
		if len(value) != size {
			t.Fatalf("value size = %d; want %d", len(value), size)
		}
	}
}

func TestPercentileMilliseconds(t *testing.T) {
	values := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}
	if got := percentileMilliseconds(values, 0.50); got != 2 {
		t.Fatalf("p50 = %v; want 2", got)
	}
	if got := percentileMilliseconds(values, 0.99); got != 3 {
		t.Fatalf("p99 = %v; want 3 with nearest-rank index used by the benchmark", got)
	}
}
