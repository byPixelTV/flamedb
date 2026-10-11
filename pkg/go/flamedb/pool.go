package flamedb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// CommandTiming separates admission wait from transport work (including auth).
type CommandTiming struct {
	Command     string
	Read        bool
	QueueWait   time.Duration
	RequestTime time.Duration
	Success     bool
}

// Client owns reserved read/write connections. Each channel lease exclusively
// owns one connection and its response until the transport operation finishes.
type Client struct {
	cfg         Config
	reads       chan *connection
	writes      chan *connection
	connections []*connection
	lifetime    context.Context
	cancel      context.CancelFunc
	closeOnce   sync.Once
}

// New authenticates one connection; other pool connections are opened lazily.
func New(cfg Config) (*Client, error) {
	if strings.ContainsAny(cfg.APIKey, "\r\n") || cfg.timeout() <= 0 {
		return nil, fmt.Errorf("flamedb: invalid API key or timeout")
	}
	if cfg.ReadConnections == 0 && cfg.WriteConnections == 0 {
		cfg.ReadConnections = 2
	}
	if cfg.WriteConnections == 0 {
		cfg.WriteConnections = 1
	}
	if cfg.ReadConnections < 0 || cfg.ReadConnections > 1024 || cfg.WriteConnections < 1 || cfg.WriteConnections > 1024 {
		return nil, fmt.Errorf("flamedb: invalid pool sizes")
	}
	c := &Client{cfg: cfg}
	c.lifetime, c.cancel = context.WithCancel(context.Background())
	makePool := func(size int) chan *connection {
		pool := make(chan *connection, size)
		for i := 0; i < size; i++ {
			conn := &connection{cfg: cfg}
			c.connections = append(c.connections, conn)
			pool <- conn
		}
		return pool
	}
	c.writes = makePool(cfg.WriteConnections)
	c.reads = c.writes
	if cfg.ReadConnections > 0 {
		c.reads = makePool(cfg.ReadConnections)
	}
	ctx, cancel := context.WithTimeout(c.lifetime, cfg.timeout())
	defer cancel()
	if err := c.connections[0].dial(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Close immediately wakes queued calls and closes active sockets.
func (c *Client) Close() (result error) {
	c.closeOnce.Do(func() {
		c.cancel()
		for _, conn := range c.connections {
			if err := conn.close(); err != nil && result == nil {
				result = err
			}
		}
	})
	return result
}

func isReadCommand(name string) bool {
	switch name {
	case "GET", "LEADERBOARD", "GROUP_LEADERBOARD", "STATS", "METRICS", "PREVIEW_DELETE":
		return true
	default:
		return false
	}
}

func (c *Client) command(ctx context.Context, line string) (map[string]json.RawMessage, error) {
	return c.commandMulti(ctx, []string{line})
}

func (c *Client) commandMulti(parent context.Context, lines []string) (result map[string]json.RawMessage, err error) {
	for _, line := range lines {
		if strings.ContainsAny(line, "\r\n\x00\x1f") {
			return nil, fmt.Errorf("flamedb: invalid command framing")
		}
	}
	if c.lifetime.Err() != nil {
		return nil, fmt.Errorf("flamedb: client closed")
	}
	started := time.Now()
	name := strings.Fields(lines[0])[0]
	read := isReadCommand(name)
	pool := c.writes
	if read {
		pool = c.reads
	}
	ctx, cancel := context.WithTimeout(parent, c.cfg.timeout())
	stopClose := context.AfterFunc(c.lifetime, cancel)
	defer func() { stopClose(); cancel() }()
	var conn *connection
	var acquired time.Time
	defer func() {
		finished := time.Now()
		timing := CommandTiming{Command: name, Read: read, Success: err == nil}
		if conn == nil {
			timing.QueueWait = finished.Sub(started)
		} else {
			timing.QueueWait = acquired.Sub(started)
			timing.RequestTime = finished.Sub(acquired)
			pool <- conn
		}
		if c.cfg.OnCommandTiming != nil {
			func() { defer func() { _ = recover() }(); c.cfg.OnCommandTiming(timing) }()
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.lifetime.Done():
		return nil, fmt.Errorf("flamedb: client closed")
	case conn = <-pool:
		acquired = time.Now()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return conn.command(ctx, lines)
}
