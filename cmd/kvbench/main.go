// Command kvbench drives concurrent requests against any node in a running
// cluster and reports observed throughput and latency percentiles.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type summary struct {
	Target       string            `json:"target"`
	Concurrency  int               `json:"concurrency"`
	Duration     string            `json:"duration"`
	Workload     string            `json:"workload"`
	Keyspace     int               `json:"keyspace"`
	ValueBytes   int               `json:"value_bytes"`
	Operations   uint64            `json:"operations"`
	Errors       uint64            `json:"errors"`
	ErrorSummary map[string]uint64 `json:"error_summary,omitempty"`
	OpsPerSecond float64           `json:"ops_per_second"`
	ErrorRatePct float64           `json:"error_rate_pct"`
	P50MS        float64           `json:"p50_ms"`
	P95MS        float64           `json:"p95_ms"`
	P99MS        float64           `json:"p99_ms"`
}

func main() {
	target := flag.String("target", "http://127.0.0.1:8081", "base URL of any cluster node")
	concurrency := flag.Int("concurrency", 50, "number of concurrent clients")
	duration := flag.Duration("duration", 10*time.Second, "benchmark duration")
	workload := flag.String("workload", "mixed", "workload: mixed, write, or read")
	keyspace := flag.Int("keyspace", 128, "number of keys shared by clients")
	valueBytes := flag.Int("value-bytes", 64, "value size for seeded keys and writes")
	requestTimeout := flag.Duration("request-timeout", 5*time.Second, "per-request timeout")
	jsonOutput := flag.Bool("json", false, "emit machine-readable JSON")
	flag.Parse()
	if *concurrency <= 0 || *duration <= 0 || *keyspace <= 0 || *valueBytes < 0 || *requestTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "concurrency, duration, keyspace, and request-timeout must be positive; value-bytes must not be negative")
		os.Exit(2)
	}
	if *workload != "mixed" && *workload != "write" && *workload != "read" {
		fmt.Fprintln(os.Stderr, "workload must be mixed, write, or read")
		os.Exit(2)
	}

	transport := &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: *requestTimeout}
	defer transport.CloseIdleConnections()
	value := strings.Repeat("x", *valueBytes)
	if *workload == "mixed" || *workload == "read" {
		seedCtx, seedCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := seedKeys(seedCtx, client, *target, *keyspace, value); err != nil {
			seedCancel()
			fmt.Fprintf(os.Stderr, "seed benchmark keys: %v\n", err)
			os.Exit(1)
		}
		seedCancel()
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	start := make(chan struct{})
	var operations atomic.Uint64
	var failures atomic.Uint64
	var failureMu sync.Mutex
	failureSummary := make(map[string]uint64)
	recordFailure := func(reason string) {
		failures.Add(1)
		failureMu.Lock()
		failureSummary[reason]++
		failureMu.Unlock()
	}
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, *concurrency*1000)
	var workers sync.WaitGroup
	workers.Add(*concurrency)

	startedAt := time.Now()
	for worker := range *concurrency {
		worker := worker
		go func() {
			defer workers.Done()
			<-start
			sequence := 0
			for ctx.Err() == nil {
				key := fmt.Sprintf("bench-%d", (worker+sequence)%*keyspace)
				method := http.MethodGet
				var body io.Reader
				if *workload == "write" || *workload == "mixed" && sequence%2 == 0 {
					method = http.MethodPut
					payload, _ := json.Marshal(map[string]string{
						"value": valueWithNonce(value, rand.Uint64()),
					})
					body = bytes.NewReader(payload)
				}
				request, err := http.NewRequestWithContext(ctx, method, *target+"/v1/kv/"+key, body)
				if err != nil {
					recordFailure("request_build")
					continue
				}
				if method == http.MethodPut {
					request.Header.Set("Content-Type", "application/json")
				}
				requestStarted := time.Now()
				response, err := client.Do(request)
				latency := time.Since(requestStarted)
				if err != nil {
					if ctx.Err() == nil {
						recordFailure("transport")
					}
					continue
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode < 200 || response.StatusCode >= 300 {
					recordFailure(fmt.Sprintf("http_%d", response.StatusCode))
					continue
				}
				operations.Add(1)
				latencyMu.Lock()
				latencies = append(latencies, latency)
				latencyMu.Unlock()
				sequence++
			}
		}()
	}
	close(start)
	workers.Wait()
	elapsed := time.Since(startedAt)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	totalAttempts := operations.Load() + failures.Load()
	errorRate := 0.0
	if totalAttempts > 0 {
		errorRate = 100 * float64(failures.Load()) / float64(totalAttempts)
	}
	result := summary{
		Target:       *target,
		Concurrency:  *concurrency,
		Duration:     elapsed.Round(time.Millisecond).String(),
		Workload:     *workload,
		Keyspace:     *keyspace,
		ValueBytes:   *valueBytes,
		Operations:   operations.Load(),
		Errors:       failures.Load(),
		ErrorSummary: failureSummary,
		OpsPerSecond: float64(operations.Load()) / elapsed.Seconds(),
		ErrorRatePct: errorRate,
		P50MS:        percentileMilliseconds(latencies, 0.50),
		P95MS:        percentileMilliseconds(latencies, 0.95),
		P99MS:        percentileMilliseconds(latencies, 0.99),
	}
	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(result)
		return
	}
	fmt.Printf("target:          %s\n", result.Target)
	fmt.Printf("concurrency:     %d\n", result.Concurrency)
	fmt.Printf("duration:        %s\n", result.Duration)
	fmt.Printf("workload:        %s (%d keys, %d-byte values)\n", result.Workload, result.Keyspace, result.ValueBytes)
	fmt.Printf("operations:      %d\n", result.Operations)
	fmt.Printf("errors:          %d\n", result.Errors)
	fmt.Printf("error rate:      %.2f%%\n", result.ErrorRatePct)
	fmt.Printf("throughput:      %.1f ops/s\n", result.OpsPerSecond)
	fmt.Printf("latency p50/p95: %.2f / %.2f ms\n", result.P50MS, result.P95MS)
	fmt.Printf("latency p99:     %.2f ms\n", result.P99MS)
	if len(result.ErrorSummary) > 0 {
		fmt.Printf("error summary:   %v\n", result.ErrorSummary)
	}
}

func seedKeys(ctx context.Context, client *http.Client, target string, keyspace int, value string) error {
	for index := range keyspace {
		payload, err := json.Marshal(map[string]string{"value": value})
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("%s/v1/kv/bench-%d", target, index), bytes.NewReader(payload))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Errorf("seed key %d returned %s", index, response.Status)
		}
	}
	return nil
}

func valueWithNonce(base string, nonce uint64) string {
	if len(base) == 0 {
		return ""
	}
	suffix := fmt.Sprintf("%016x", nonce)
	if len(base) <= len(suffix) {
		return suffix[:len(base)]
	}
	return base[:len(base)-len(suffix)] + suffix
}

func percentileMilliseconds(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * percentile)
	return float64(values[index]) / float64(time.Millisecond)
}
