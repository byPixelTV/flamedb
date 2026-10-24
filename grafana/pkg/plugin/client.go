package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
type taggedSeries struct {
	Tags   map[string]string `json:"tags"`
	Points []point           `json:"points"`
}
type taggedValue struct {
	Tags  map[string]string `json:"tags"`
	TS    int64             `json:"ts"`
	Value float64           `json:"value"`
}
type groupedReply struct {
	IntervalNS int64          `json:"interval_ns"`
	Series     []taggedSeries `json:"series"`
	Latest     []taggedValue  `json:"latest"`
}
type reply struct {
	Grouped *groupedReply `json:"grouped"`

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
	reader := bufio.NewReaderSize(conn, 64*1024)
	read := func() (*reply, error) {
		r, err := readReply(reader)
		if err != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return r, err
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

const maxResponseBytes = 16 * 1024 * 1024

var errResponseTooLarge = errors.New("FlameDB response exceeds the 16 MiB frame limit")

// Read an entire newline-delimited frame with an explicit byte ceiling. Scanner's
// token error was ambiguous and could also originate in an upstream cluster hop.
func readReply(reader *bufio.Reader) (*reply, error) {
	var raw []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		if len(raw)+len(chunk) > maxResponseBytes {
			return nil, errResponseTooLarge
		}
		raw = append(raw, chunk...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			if err == io.EOF && len(raw) > 0 {
				return nil, io.ErrUnexpectedEOF
			}
			if err == io.EOF {
				return nil, errors.New("FlameDB closed the connection")
			}
			return nil, err
		}
		break
	}
	var r reply
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("invalid FlameDB response: %w", err)
	}
	if r.Error != "" {
		if strings.Contains(r.Error, "bufio.Scanner: token too long") {
			return nil, fmt.Errorf("%w: upstream response limit exceeded", errResponseTooLarge)
		}
		return nil, errors.New(r.Error)
	}
	return &r, nil
}
