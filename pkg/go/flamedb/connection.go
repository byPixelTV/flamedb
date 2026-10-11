package flamedb

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

type connection struct {
	cfg       Config
	lifecycle sync.Mutex
	conn      net.Conn
	scanner   *bufio.Scanner
	writer    *bufio.Writer
	authed    bool
	closed    bool
}

func (c *connection) close() error {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	c.closed = true
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *connection) invalidate(conn net.Conn) {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	conn.Close()
	if c.conn == conn {
		c.conn = nil
	}
}

func (c *connection) dial(ctx context.Context) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.cfg.addr())
	if err != nil {
		return fmt.Errorf("flamedb: connect: %w", err)
	}
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
	}()
	c.lifecycle.Lock()
	if c.closed {
		c.lifecycle.Unlock()
		conn.Close()
		return fmt.Errorf("flamedb: client closed")
	}
	c.conn = conn
	c.lifecycle.Unlock()
	c.scanner = bufio.NewScanner(conn)
	c.scanner.Buffer(make([]byte, 0, 64*1024), 4<<20)
	c.writer = bufio.NewWriter(conn)
	if err := c.auth(); err != nil {
		c.invalidate(conn)
		return err
	}
	return nil
}

func (c *connection) command(ctx context.Context, lines []string) (map[string]json.RawMessage, error) {
	c.lifecycle.Lock()
	conn, closed := c.conn, c.closed
	c.lifecycle.Unlock()
	if closed {
		return nil, fmt.Errorf("flamedb: client closed")
	}
	if conn == nil {
		if err := c.dial(ctx); err != nil {
			return nil, fmt.Errorf("flamedb: reconnect failed; command was not sent: %w", err)
		}
		c.lifecycle.Lock()
		conn = c.conn
		c.lifecycle.Unlock()
	}
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.sendLines(lines); err != nil {
		c.invalidate(conn)
		return nil, fmt.Errorf("flamedb: command failed; outcome unknown; not retried: %w", err)
	}
	raw, err := c.readLine()
	if err != nil {
		c.invalidate(conn)
		return nil, fmt.Errorf("flamedb: command failed; outcome unknown; not retried: %w", err)
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		c.invalidate(conn)
		return nil, fmt.Errorf("flamedb: parse response; outcome unknown; not retried: %w", err)
	}
	if errMsg, ok := result["error"]; ok {
		var msg string
		_ = json.Unmarshal(errMsg, &msg)
		return nil, fmt.Errorf("flamedb: %s", msg)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *connection) auth() error {
	// read {"auth":"required"}
	line, err := c.readLine()
	if err != nil {
		return err
	}
	var challenge map[string]string
	if err := json.Unmarshal([]byte(line), &challenge); err != nil || challenge["auth"] != "required" {
		return fmt.Errorf("flamedb: unexpected auth challenge: %s", line)
	}

	if err := c.sendLine("AUTH " + c.cfg.APIKey); err != nil {
		return err
	}

	resp, err := c.readLine()
	if err != nil {
		return err
	}
	var authResp map[string]string
	if err := json.Unmarshal([]byte(resp), &authResp); err != nil {
		return fmt.Errorf("flamedb: auth parse error: %w", err)
	}
	if authResp["auth"] != "ok" {
		return fmt.Errorf("flamedb: auth failed: %s", authResp["error"])
	}
	c.authed = true
	return nil
}

func (c *connection) readLine() (string, error) {

	if !c.scanner.Scan() {
		if err := c.scanner.Err(); err != nil {
			return "", fmt.Errorf("flamedb: read: %w", err)
		}
		return "", fmt.Errorf("flamedb: connection closed")
	}
	return c.scanner.Text(), nil
}

func (c *connection) sendLine(line string) error {

	if _, err := c.writer.WriteString(line + "\n"); err != nil {
		return fmt.Errorf("flamedb: write: %w", err)
	}
	return c.writer.Flush()
}

func (c *connection) sendLines(lines []string) error {

	for _, l := range lines {
		if _, err := c.writer.WriteString(l + "\n"); err != nil {
			return fmt.Errorf("flamedb: write: %w", err)
		}
	}
	return c.writer.Flush()
}
