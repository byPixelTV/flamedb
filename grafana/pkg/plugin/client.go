package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

// Each request owns its socket. Bounded concurrency avoids interleaved protocol
// replies and keeps cancellation independent of other panels.
type wireClient struct {
	address, apiKey string
	timeout         time.Duration
	slots           chan struct{}
}

type event struct {
	Timestamp int64             `json:"timestamp"`
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}
type point struct {
	TS    int64   `json:"ts"`
	Value float64 `json:"value"`
	Count int     `json:"count"`
}
type reply struct {
	Error      string   `json:"error"`
	Auth       string   `json:"auth"`
	MetricKeys []string `json:"metric_keys"`
	Events     []event  `json:"events"`
	Series     []point  `json:"series"`
}

func (c *wireClient) query(parent context.Context, command string) (*reply, error) {
	// Commands are generated internally; never accept arbitrary protocol lines.
	if strings.ContainsAny(command, "\r\n\x00\x1f") {
		return nil, errors.New("invalid command framing")
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.address)
	if err != nil {
		return nil, fmt.Errorf("connect to FlameDB: %w", err)
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	read := func() (*reply, error) {
		if !scanner.Scan() {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if scanner.Err() != nil {
				return nil, scanner.Err()
			}
			return nil, errors.New("FlameDB closed the connection")
		}
		var r reply
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("invalid FlameDB response: %w", err)
		}
		if r.Error != "" {
			return nil, errors.New(r.Error)
		}
		return &r, nil
	}
	greeting, err := read()
	if err != nil {
		return nil, err
	}
	if greeting.Auth != "required" {
		return nil, errors.New("unexpected FlameDB authentication handshake")
	}
	if _, err = fmt.Fprintf(conn, "AUTH %s\n", c.apiKey); err != nil {
		return nil, err
	}
	auth, err := read()
	if err != nil {
		return nil, err
	}
	if auth.Auth != "ok" {
		return nil, errors.New("FlameDB authentication failed")
	}
	if _, err = fmt.Fprintln(conn, command); err != nil {
		return nil, err
	}
	return read()
}
