package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/cluster"
	"github.com/byPixelTV/flamedb/internal/config"
	"github.com/byPixelTV/flamedb/internal/storage"
)

func TestServer_ClusterJoin(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.Open(filepath.Join(tmpDir, "data"))
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	defer store.Close()

	lb := aggregates.New(store.DB())

	authCfg := config.AuthConfig{
		Keys: []config.APIKey{
			{
				Name:        "test-node",
				Key:         "test_key_123",
				Permissions: []string{"read", "write"},
			},
		},
	}
	a := auth.New(authCfg)

	self := cluster.Node{
		ID:   "node-1",
		Addr: "127.0.0.1:9001",
	}
	c := cluster.New(self, 150)
	internalKey := "test_key_123"

	srv := New(store, lb, a, c, internalKey)

	// Listen on dynamic port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		srv.handleConn(conn)
	}()

	// Client connection
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// 1. Read auth required
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read auth prompt: %v", err)
	}
	if !strings.Contains(line, `"auth":"required"`) {
		t.Fatalf("unexpected auth prompt: %q", line)
	}

	// 2. Send auth
	fmt.Fprintf(conn, "AUTH %s\n", internalKey)

	// 3. Read auth ok
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read auth response: %v", err)
	}
	if !strings.Contains(line, `"auth":"ok"`) {
		t.Fatalf("unexpected auth response: %q", line)
	}

	// 4. Send CLUSTER message
	joinMsg := cluster.DiscoveryMessage{
		Type:   "JOIN",
		NodeID: "node-2",
		Addr:   "127.0.0.1:9002",
	}
	data, _ := json.Marshal(joinMsg)
	fmt.Fprintf(conn, "CLUSTER %s\n", string(data))

	// 5. Read cluster response
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read cluster response: %v", err)
	}
	if !strings.Contains(line, `"cluster":"ok"`) {
		t.Fatalf("unexpected cluster response: %q", line)
	}
}
