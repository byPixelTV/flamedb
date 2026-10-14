package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFrameSelectsTimestampModeWithoutChangingEvents(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		o := options{mode: "write", prefix: "timestamp_mode", metrics: 16, entities: 1500, batch: 100, leaderboard: true, automaticTimestamps: automatic}
		s := &counters{}
		frame, p := frame(o, s, 0, 0)
		if strings.Contains(frame, " ts=") == automatic || strings.Count(frame, "WRITE timestamp_mode.") != 100 || strings.Count(frame, " region=\"eu\"") != 100 || len(p.metrics) != 100 || len(p.entities) != 100 || s.seq.Load() != 100 {
			t.Fatalf("incorrect timestamp workload automatic=%t", automatic)
		}
		if !strings.HasPrefix(frame, "WRITE_BATCH\n") || !strings.HasSuffix(frame, "END\n") {
			t.Fatal("batch framing changed")
		}
	}
}

// A malformed acknowledgment must never inflate confirmed/durable counts,
// and every mutation already pipelined behind it remains ambiguous.
func TestMalformedAcknowledgmentAccountsForEntirePipeline(t *testing.T) {
	for _, tc := range []struct {
		name, reply string
		batch       int
	}{
		{"single", `{"ok":true}`, 1},
		{"batch-count", `{"ok":true,"accepted":2,"failed":0}`, 3},
		{"batch-ok", `{"ok":false,"accepted":3,"failed":0}`, 3},
		{"batch-index", `{"ok":false,"accepted":2,"failed":1,"errors":[{"index":4}]}`, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				reader := bufio.NewReader(conn)
				fmt.Fprintln(conn, `{"auth":"required"}`)
				if _, err := reader.ReadString('\n'); err != nil {
					done <- err
					return
				}
				fmt.Fprintln(conn, `{"auth":"ok"}`)
				lines := 3
				if tc.batch > 1 {
					lines *= tc.batch + 2
				}
				for i := 0; i < lines; i++ {
					if _, err := reader.ReadString('\n'); err != nil {
						done <- err
						return
					}
				}
				_, err = fmt.Fprintln(conn, tc.reply)
				done <- err
			}()
			o := options{addrs: listener.Addr().String(), key: "test", mode: "write", prefix: "test", batch: tc.batch, pipeline: 3, metrics: 1, entities: 1, timeout: 5 * time.Second}
			s := &counters{expected: make([]atomic.Int64, 1)}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			wg.Add(1)
			worker(ctx, o, s, 0, &wg)
			wg.Wait()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if s.confirmed.Load() != 0 || s.confirmedRequests.Load() != 0 || s.expected[0].Load() != 0 || s.errors.Load() != 1 || s.ambiguous.Load() != int64(3*tc.batch) {
				t.Fatalf("confirmed=%d requests=%d expected=%d errors=%d ambiguous=%d", s.confirmed.Load(), s.confirmedRequests.Load(), s.expected[0].Load(), s.errors.Load(), s.ambiguous.Load())
			}
		})
	}
}
