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
	"sync"
	"sync/atomic"
	"time"
)

type summary struct {
	Target       string  `json:"target"`
	Concurrency  int     `json:"concurrency"`
	Duration     string  `json:"duration"`
	Operations   uint64  `json:"operations"`
	Errors       uint64  `json:"errors"`
	OpsPerSecond float64 `json:"ops_per_second"`
	P50MS        float64 `json:"p50_ms"`
	P95MS        float64 `json:"p95_ms"`
	P99MS        float64 `json:"p99_ms"`
}

func main() {
	target := flag.String("target", "http://127.0.0.1:8081", "base URL of any cluster node")
	concurrency := flag.Int("concurrency", 50, "number of concurrent clients")
	duration := flag.Duration("duration", 10*time.Second, "benchmark duration")
	jsonOutput := flag.Bool("json", false, "emit machine-readable JSON")
	flag.Parse()
	if *concurrency <= 0 || *duration <= 0 {
		fmt.Fprintln(os.Stderr, "concurrency and duration must be positive")
		os.Exit(2)
	}

	transport := &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	defer transport.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	start := make(chan struct{})
	var operations atomic.Uint64
	var failures atomic.Uint64
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
				// Each pair targets the same key: an even sequence writes it and
				// the following odd sequence reads that committed value.
				key := fmt.Sprintf("bench-%d-%d", worker, (sequence/2)%32)
				method := http.MethodPut
				var body io.Reader
				if sequence%2 == 0 {
					payload, _ := json.Marshal(map[string]string{
						"value": fmt.Sprintf("value-%d", rand.Uint64()),
					})
					body = bytes.NewReader(payload)
				} else {
					method = http.MethodGet
				}
				request, err := http.NewRequestWithContext(ctx, method, *target+"/v1/kv/"+key, body)
				if err != nil {
					failures.Add(1)
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
						failures.Add(1)
					}
					continue
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode < 200 || response.StatusCode >= 300 {
					failures.Add(1)
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

	result := summary{
		Target:       *target,
		Concurrency:  *concurrency,
		Duration:     elapsed.Round(time.Millisecond).String(),
		Operations:   operations.Load(),
		Errors:       failures.Load(),
		OpsPerSecond: float64(operations.Load()) / elapsed.Seconds(),
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
	fmt.Printf("operations:      %d\n", result.Operations)
	fmt.Printf("errors:          %d\n", result.Errors)
	fmt.Printf("throughput:      %.1f ops/s\n", result.OpsPerSecond)
	fmt.Printf("latency p50/p95: %.2f / %.2f ms\n", result.P50MS, result.P95MS)
	fmt.Printf("latency p99:     %.2f ms\n", result.P99MS)
}

func percentileMilliseconds(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * percentile)
	return float64(values[index]) / float64(time.Millisecond)
}
