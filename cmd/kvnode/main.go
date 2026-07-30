package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jiholee5217/distributed-kv-store/internal/node"
	"github.com/jiholee5217/distributed-kv-store/internal/ring"
)

func main() {
	var (
		id         = flag.String("id", "node-1", "unique node ID")
		listen     = flag.String("listen", ":8080", "HTTP listen address")
		advertise  = flag.String("advertise", "http://127.0.0.1:8080", "URL other nodes use to reach this node")
		memberList = flag.String("members", "", "comma-separated id=url peers; this node is added automatically")
		replicas   = flag.Int("replicas", 3, "number of replicas per key")
		readQ      = flag.Int("read-quorum", 0, "successful replica responses required per read (default: majority)")
		writeQ     = flag.Int("write-quorum", 0, "successful acknowledgements required per write (default: majority)")
		timeout    = flag.Duration("request-timeout", 1500*time.Millisecond, "per-replica request timeout")
	)
	flag.Parse()

	members, err := parseMembers(*memberList)
	if err != nil {
		exit(err)
	}
	if err := addSelf(&members, ring.Member{ID: *id, URL: strings.TrimRight(*advertise, "/")}); err != nil {
		exit(err)
	}

	effectiveReplicas := min(*replicas, len(members))
	if *readQ == 0 {
		*readQ = effectiveReplicas/2 + 1
	}
	if *writeQ == 0 {
		*writeQ = effectiveReplicas/2 + 1
	}

	n, err := node.New(node.Config{
		ID:                *id,
		Members:           members,
		ReplicationFactor: *replicas,
		ReadQuorum:        *readQ,
		WriteQuorum:       *writeQ,
		RequestTimeout:    *timeout,
	})
	if err != nil {
		exit(err)
	}

	server := &http.Server{
		Addr:              *listen,
		Handler:           n.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	shutdownSignal, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-shutdownSignal.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("graceful shutdown failed", "error", err)
		}
	}()

	slog.Info("node starting",
		"id", *id,
		"listen", *listen,
		"advertise", *advertise,
		"members", len(members),
		"replicas", effectiveReplicas,
		"read_quorum", *readQ,
		"write_quorum", *writeQ,
	)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exit(err)
	}
}

func parseMembers(input string) ([]ring.Member, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}

	var members []ring.Member
	for _, entry := range strings.Split(input, ",") {
		id, address, ok := strings.Cut(strings.TrimSpace(entry), "=")
		if !ok || id == "" || address == "" {
			return nil, fmt.Errorf("invalid member %q; expected id=url", entry)
		}
		address = strings.TrimRight(address, "/")
		parsed, err := url.ParseRequestURI(address)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("invalid URL for member %q", id)
		}
		members = append(members, ring.Member{ID: id, URL: address})
	}
	return members, nil
}

func addSelf(members *[]ring.Member, self ring.Member) error {
	parsed, err := url.ParseRequestURI(self.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("advertise must be an absolute HTTP URL")
	}
	for _, member := range *members {
		if member.ID == self.ID {
			if member.URL != self.URL {
				return fmt.Errorf("node ID %q has conflicting URLs", self.ID)
			}
			return nil
		}
	}
	*members = append(*members, self)
	return nil
}

func exit(err error) {
	slog.Error("fatal error", "error", err)
	os.Exit(1)
}
