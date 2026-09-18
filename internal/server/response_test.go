package server

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestWriteAckRefreshesExpiredDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	// Simulate the deadline left by an earlier JSON response expiring while idle.
	if err := server.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	client.SetReadDeadline(time.Now().Add(time.Second))
	done := make(chan struct{})
	go func() { writeEmptyResult(server); close(done) }()
	line, err := bufio.NewReader(client).ReadString('\n')
	<-done
	if err != nil || line != "{}\n" {
		t.Fatalf("missing write ACK: %q %v", line, err)
	}
}

// Track deadline cleanup without waiting for a real 30-second idle period.
type observedResponseConn struct {
	net.Conn
	deadline time.Time
	closed   bool
}

func (c *observedResponseConn) SetWriteDeadline(deadline time.Time) error {
	c.deadline = deadline
	return c.Conn.SetWriteDeadline(deadline)
}
func (c *observedResponseConn) Close() error { c.closed = true; return c.Conn.Close() }

func TestResponsePathsClearDeadline(t *testing.T) {
	for _, kind := range []string{"json", "empty", "forwarded"} {
		t.Run(kind, func(t *testing.T) {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			conn := &observedResponseConn{Conn: server}
			client.SetReadDeadline(time.Now().Add(time.Second))
			done := make(chan struct{})
			go func() {
				switch kind {
				case "json":
					writeJSON(conn, map[string]string{"ok": "yes"})
				case "empty":
					writeEmptyResult(conn)
				case "forwarded":
					writeResponse(conn, []byte("{}\n"))
				}
				close(done)
			}()
			if _, err := bufio.NewReader(client).ReadString('\n'); err != nil {
				t.Fatal(err)
			}
			<-done
			if !conn.deadline.IsZero() {
				t.Fatal("response left a stale deadline")
			}
			if conn.closed {
				t.Fatal("successful response closed connection")
			}
		})
	}
}

func TestFailedResponseClosesConnection(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	client.Close()
	conn := &observedResponseConn{Conn: server}
	writeEmptyResult(conn)
	if !conn.closed {
		t.Fatal("failed response left connection open")
	}
}
