package cluster

import (
	"bufio"
	"fmt"
	"github.com/cockroachdb/pebble/v2"
	"net"
	"testing"
	"time"
)

func TestQuorumRejectsApplicationFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		sc := bufio.NewScanner(conn)
		fmt.Fprintln(conn, `{"auth":"required"}`)
		sc.Scan()
		fmt.Fprintln(conn, `{"auth":"ok"}`)
		sc.Scan()
		fmt.Fprintln(conn, `{"error":"disk full"}`)
	}()
	c := New(Node{ID: "a"}, 150, "test", 2)
	c.AddNode(Node{ID: "b", Addr: ln.Addr().String()})
	if err := c.ReplicateQuorum("m", `WRITE m 1 ts=1 __replica`); err == nil {
		t.Fatal("counted rejected write toward quorum")
	}
	c.Close()
	<-done
}
func TestConfiguredQuorumCannotShrinkToSingleNode(t *testing.T) {
	c := New(Node{ID: "a"}, 150, "test", 3)
	defer c.Close()
	if err := c.ReplicateQuorum("m", "WRITE m 1"); err == nil {
		t.Fatal("unavailable configured majority was accepted")
	}
}

func TestOutboxRecoveryDeliversAndDeletes(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	received := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sc := bufio.NewScanner(conn)
		fmt.Fprintln(conn, `{"auth":"required"}`)
		sc.Scan()
		fmt.Fprintln(conn, `{"auth":"ok"}`)
		if sc.Scan() {
			received <- sc.Text()
			fmt.Fprintln(conn, `{}`)
		}
	}()
	path := t.TempDir()
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := newReplicationOutbox(db).put(Node{ID: "b", Addr: ln.Addr().String()}, `WRITE m 1 ts=123 __replica`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := New(Node{ID: "a"}, 150, "test", 2)
	c.AttachReplicationOutbox(db)
	defer c.Close()
	select {
	case line := <-received:
		if line != `WRITE m 1 ts=123 __replica` {
			t.Fatal(line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recovery did not deliver")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, closer, err := db.Get(key)
		if err == pebble.ErrNotFound {
			break
		}
		if err == nil {
			closer.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("acknowledged record retained")
		}
		time.Sleep(time.Millisecond)
	}
}
