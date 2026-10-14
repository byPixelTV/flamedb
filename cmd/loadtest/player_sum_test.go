package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPlayerSumTargetsCoverAllPlayersAcrossWorkers(t *testing.T) {
	o := options{mode: "player-sum", metrics: 5, entities: 1500, readCommands: make([]string, 7500)}
	for i := range o.readCommands {
		o.readCommands[i] = fmt.Sprintf("target%d", i)
	}
	s := &counters{}
	seen := make([]atomic.Int64, 7500)
	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 7500; i++ {
				line, p := frame(o, s, worker, i)
				if !p.read || line != o.readCommands[p.fixtureMetric]+"\n" {
					t.Error("incorrect player read")
				}
				seen[p.fixtureMetric].Add(1)
			}
		}(worker)
	}
	wg.Wait()
	for i := range seen {
		if seen[i].Load() != 32 {
			t.Fatalf("target %d visited %d times", i, seen[i].Load())
		}
	}
	o.mode = "player-mixed"
	o.prefix = "mixed"
	o.batch = 128
	o.leaderboard = true
	line, p := frame(o, s, 0, 0)
	if p.read || strings.Count(line, "WRITE mixed.") != 128 {
		t.Fatal("mixed write not batched")
	}
	line, p = frame(o, s, 0, 4)
	if !p.read || line != o.readCommands[p.fixtureMetric]+"\n" {
		t.Fatal("mixed player read missing")
	}
}

func TestPlayerSumReadbackRejectsWrongPlayerHistory(t *testing.T) {
	for _, wrong := range []bool{false, true} {
		t.Run(fmt.Sprint(wrong), func(t *testing.T) {
			clientConn, server := net.Pipe()
			defer clientConn.Close()
			defer server.Close()
			c := &client{Conn: clientConn, reader: bufio.NewReader(clientConn)}
			o := options{prefix: "players", metrics: 1, entities: 2, timeout: time.Second}
			s := &counters{expectedScores: make([]atomic.Int64, 2)}
			s.expectedScores[0].Store(3)
			s.expectedScores[1].Store(7)
			done := make(chan error, 1)
			go func() {
				reader := bufio.NewReader(server)
				for player := 0; player < 2; player++ {
					line, err := reader.ReadString('\n')
					if err != nil {
						done <- err
						return
					}
					if line != fmt.Sprintf("GET players.0 SUM WHERE player=\"player%d\" AND region=\"eu\"\n", player) {
						done <- fmt.Errorf("wrong filter: %s", line)
						return
					}
					count := []int{3, 7}[player]
					if wrong {
						count = 10
					}
					fmt.Fprintf(server, "{\"aggregate\":{\"type\":\"SUM\",\"count\":%d,\"value\":%d}}\n", count, count)
					if wrong {
						done <- nil
						return
					}
				}
				done <- nil
			}()
			err := verifyPlayerSums(c, o, s)
			if (err != nil) != wrong {
				t.Fatalf("wrong=%t error=%v", wrong, err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
