package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jiholee5217/distributed-kv-store/internal/raft"
)

func main() {
	var (
		id             = flag.String("id", "node-1", "unique Raft node ID")
		listen         = flag.String("listen", ":8080", "HTTP listen address")
		advertise      = flag.String("advertise", "http://127.0.0.1:8080", "URL other nodes use to reach this node")
		memberList     = flag.String("members", "", "comma-separated id=url peers; this node is added automatically")
		dataDir        = flag.String("data-dir", "", "persistent state directory (default: data/<node-id>)")
		electionMin    = flag.Duration("election-min", 350*time.Millisecond, "minimum randomized election timeout")
		electionMax    = flag.Duration("election-max", 700*time.Millisecond, "maximum randomized election timeout")
		heartbeat      = flag.Duration("heartbeat", 100*time.Millisecond, "leader heartbeat interval")
		rpcTimeout     = flag.Duration("rpc-timeout", 250*time.Millisecond, "peer RPC timeout")
		forwardTimeout = flag.Duration("forward-timeout", 10*time.Second, "forwarded client request timeout")
	)
	flag.Parse()

	members, err := parseMembers(*memberList)
	if err != nil {
		exit(err)
	}
	if err := addSelf(&members, raft.Member{ID: *id, URL: strings.TrimRight(*advertise, "/")}); err != nil {
		exit(err)
	}
	if *dataDir == "" {
		*dataDir = filepath.Join("data", *id)
	}
	storage, err := raft.NewFileStorage(*dataDir)
	if err != nil {
		exit(err)
	}
	node, err := raft.NewNode(raft.Config{
		ID:                 *id,
		Members:            members,
		Storage:            storage,
		ElectionTimeoutMin: *electionMin,
		ElectionTimeoutMax: *electionMax,
		HeartbeatInterval:  *heartbeat,
		RPCTimeout:         *rpcTimeout,
		ForwardTimeout:     *forwardTimeout,
	})
	if err != nil {
		exit(err)
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		exit(err)
	}
	server := &http.Server{
		Handler:           node.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	node.Start()

	shutdownSignal, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	go func() {
		<-shutdownSignal.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("graceful HTTP shutdown failed", "error", err)
		}
		node.Close()
	}()

	slog.Info("Raft node starting",
		"id", *id,
		"listen", *listen,
		"advertise", *advertise,
		"members", len(members),
		"data_dir", *dataDir,
	)
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exit(err)
	}
}

func parseMembers(input string) ([]raft.Member, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}
	members := make([]raft.Member, 0, len(strings.Split(input, ",")))
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
		members = append(members, raft.Member{ID: id, URL: address})
	}
	return members, nil
}

func addSelf(members *[]raft.Member, self raft.Member) error {
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
