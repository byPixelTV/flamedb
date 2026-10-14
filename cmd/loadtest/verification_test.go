package main

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryVerificationRejectsWrongScores(t *testing.T) {
	for _, tc := range []struct {
		name, board string
		valid       bool
	}{
		{"positive", `[{"entity_id":"player0","value":3}]`, true},
		{"wrong-score", `[{"entity_id":"player0","value":2}]`, false},
		{"missing", `[]`, false},
		{"extra", `[{"entity_id":"player0","value":3},{"entity_id":"player1","value":1}]`, false},
		{"duplicate", `[{"entity_id":"player0","value":3},{"entity_id":"player0","value":3}]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan error, 1)
			go func() {
				c, err := listener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(3 * time.Second))
				r := bufio.NewReader(c)
				fmt.Fprintln(c, `{"auth":"required"}`)
				for _, response := range []string{`{"auth":"ok"}`, `{"aggregate":{"count":3,"value":3}}`, `{"leaderboard":` + tc.board + `}`} {
					if _, err := r.ReadString('\n'); err != nil {
						done <- err
						return
					}
					if _, err := fmt.Fprintln(c, response); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			file := filepath.Join(t.TempDir(), "verify.json")
			if err := saveVerification(file, verificationDocument{Version: 1, Boards: true, Metrics: []verificationMetric{{Metric: "fixture.0", Events: 3, Scores: map[string]int64{"player0": 3}}}}); err != nil {
				t.Fatal(err)
			}
			err = verifySaved(options{addrs: listener.Addr().String(), key: "test", verifyFile: file, timeout: 3 * time.Second})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t error=%v", tc.valid, err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
