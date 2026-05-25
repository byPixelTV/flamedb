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
	"github.com/byPixelTV/flamedb/internal/util"
)

func printBanner(version string) {
	const reset = "\x1b[0m"

	lines := []string{
		" _______  _        _______  _______  _______  ______   ______  ",
		"(  ____ \\( \\      (  ___  )(       )(  ____ \\(  __  \\ (  ___ \\ ",
		"| (    \\/| (      | (   ) || () () || (    \\/| (  \\  )| (   ) )",
		"| (__    | |      | (___) || || || || (__    | |   ) || (__/ / ",
		"|  __)   | |      |  ___  || |(_)| ||  __)   | |   | ||  __ (  ",
		"| (      | |      | (   ) || |   | || (      | |   ) || (  \\ \\ ",
		"| )      | (____/\\| )   ( || )   ( || (____/\\| (__/  )| )___) )",
		"|/       (_______/|/     \\||/     \\|(_______/(______/ |/ \\___/ ",
	}

	startR, startG, startB := 255, 120, 120
	endR, endG, endB := 255, 40, 40

	for i, line := range lines {
		t := float64(i) / float64(len(lines)-1)

		r := int(float64(startR)*(1-t) + float64(endR)*t)
		g := int(float64(startG)*(1-t) + float64(endG)*t)
		b := int(float64(startB)*(1-t) + float64(endB)*t)

		color := fmt.Sprintf("\x1b[38;2;%d;%d;%dm", r, g, b)
		fmt.Println(color + line + reset)
	}

	versionColor := "\x1b[38;2;140;140;140m"
	accentColor := "\x1b[38;2;255;80;80m"

	fmt.Printf(
		"%s╰─%s v%s%s\n\n",
		versionColor,
		accentColor,
		version,
		reset,
	)
}

func main() {
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal(err)
	}

	printBanner(util.CurrentVersion())

	store, err := storage.Open(cfg.Server.DataPath, cfg.Storage.Compression)
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
	c.SetQueueSizes(cfg.Cluster.ReplicationQueueSize, cfg.Cluster.FanoutQueueSize)
	c.AttachReplicationOutbox(store.DB())

	// seeds joinen, kein pre-loading von nodes aus config
	go func() {
		time.Sleep(500 * time.Millisecond)
		c.JoinSeeds(cfg.Cluster.Seeds, internalKey)
		c.StartHeartbeat(internalKey)
		// rebalance nach join
		if len(cfg.Cluster.Seeds) > 0 {
			c.TriggerRebalance(store, internalKey)
		}
	}()

	if cfg.Cluster.ReadPolicy != "" {
		c.SetReadPolicy(cluster.ReadPolicy(cfg.Cluster.ReadPolicy))
	}

	srv := server.New(store, lb, a, c, internalKey, cfg, configPath)

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
