package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/cluster"
	"github.com/byPixelTV/flamedb/internal/config"
	"github.com/byPixelTV/flamedb/internal/server"
	"github.com/byPixelTV/flamedb/internal/storage"
)

func main() {
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal(err)
	}

	store, err := storage.Open(cfg.Server.DataPath)
	if err != nil {
		log.Fatal(err)
	}

	lb := aggregates.New(store.DB())
	a := auth.New(cfg.Auth)
	advertiseAddr := cfg.Server.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	}

	self := cluster.Node{
		ID:   cfg.Server.NodeID,
		Addr: advertiseAddr,
	}

	internalKey := cfg.Auth.Keys[0].Key

	replicationFactor := cfg.Cluster.ReplicationFactor
	if replicationFactor < 1 {
		replicationFactor = 1
	}
	c := cluster.New(self, 150, internalKey, replicationFactor)

	// seeds joinen, kein pre-loading von nodes aus config
	go func() {
		time.Sleep(500 * time.Millisecond)
		c.JoinSeeds(cfg.Cluster.Seeds, internalKey)
		c.StartHeartbeat(internalKey)
	}()

	srv := server.New(store, lb, a, c, internalKey)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("FlameDB shutting down...")
		store.Close()
		os.Exit(0)
	}()

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	log.Fatal(srv.Listen(addr))
}
