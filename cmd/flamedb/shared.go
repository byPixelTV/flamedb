package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/byPixelTV/flamedb/internal/analytics"
	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/config"
	"github.com/byPixelTV/flamedb/internal/server"
	"github.com/redis/go-redis/v9"
)

func runShared(cfg *config.Config) error {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	maintenance := len(os.Args) > 2 && os.Args[2] == "--rebuild"
	startupBudget := cfg.Server.RequestTimeout
	if maintenance {
		startupBudget = 30 * time.Minute
	}
	startup, cancel := context.WithTimeout(context.Background(), startupBudget)
	defer cancel()
	addresses := cfg.Storage.Redis.Addresses
	if len(addresses) == 0 {
		addresses = []string{cfg.Storage.Redis.Address}
	}
	ro := redis.UniversalOptions{Addrs: addresses, Username: cfg.Storage.Redis.Username, Password: cfg.Storage.Redis.Password, DB: cfg.Storage.Redis.DB, PoolSize: cfg.Storage.Redis.PoolSize}
	ro.SentinelUsername = cfg.Storage.Redis.SentinelUsername
	ro.SentinelPassword = cfg.Storage.Redis.SentinelPassword
	switch cfg.Storage.Redis.Mode {
	case "sentinel":
		ro.MasterName = cfg.Storage.Redis.MasterName
	case "cluster": // UniversalClient selects Cluster for multiple addresses; the engine also supports a single seed.
	}
	if cfg.Storage.Redis.TLS {
		ro.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	engine, err := analytics.Open(startup, analytics.Options{DSN: cfg.Storage.Postgres.DSN, MaxConnections: cfg.Storage.Postgres.MaxConnections, Timescale: cfg.Storage.Engine == "timescaledb", RedisOptions: ro, RedisMode: cfg.Storage.Redis.Mode, RequestTimeout: cfg.Server.RequestTimeout, MaxQueryEvents: cfg.Storage.MaxQueryEvents, MaxResultBytes: cfg.Server.MaxResponseBytes, BatchSize: cfg.Performance.BatchSize, QuorumReplicationFactor: cfg.Storage.Postgres.QuorumReplicationFactor})
	if err != nil {
		return err
	}
	defer engine.Close()
	if maintenance {
		rebuild, done := context.WithTimeout(context.Background(), 30*time.Minute)
		defer done()
		if err := engine.Rebuild(rebuild); err != nil {
			return err
		}
		logger.Info("Redis leaderboard rebuild completed")
		return nil
	}
	srv := server.NewShared(engine, auth.New(cfg.Auth), cfg)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	failures := make(chan error, 3)
	var monitor *http.Server
	var profiler *http.Server
	if cfg.Performance.ProfileAddress != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		for _, name := range []string{"heap", "allocs", "goroutine", "block", "mutex"} {
			mux.Handle("/debug/pprof/"+name, pprof.Handler(name))
		}
		profiler = &http.Server{Addr: cfg.Performance.ProfileAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second}
		go func() {
			err := profiler.ListenAndServe()
			if !errors.Is(err, http.ErrServerClosed) {
				failures <- err
			}
		}()
	}
	if cfg.Server.MetricsAddress != "" {
		mux := http.NewServeMux()
		health := func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"status":"ok"}`)
		}
		mux.HandleFunc("/health", health)
		mux.HandleFunc("/healthz", health)
		ready := func(w http.ResponseWriter, r *http.Request) {
			if !srv.Listening() {
				http.Error(w, "server unavailable", http.StatusServiceUnavailable)
				return
			}
			check, done := context.WithTimeout(r.Context(), 2*time.Second)
			defer done()
			if err := engine.Ready(check); err != nil {
				http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
				return
			}
			fmt.Fprintln(w, "ready")
		}
		mux.HandleFunc("/ready", ready)
		mux.HandleFunc("/readyz", ready)
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			connections, inflight, completed, failed, busy := srv.MetricsSnapshot()
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			fmt.Fprintf(w, "flamedb_connections %d\nflamedb_inflight_requests %d\nflamedb_requests_total %d\nflamedb_request_errors_total %d\nflamedb_busy_total %d\n", connections, inflight, completed, failed, busy)
			pool := engine.PoolStats()
			queued, active, merged := engine.IngestionStats()
			fmt.Fprintf(w, "flamedb_ingest_queue_requests %d\nflamedb_ingest_active_waves %d\nflamedb_ingest_coalesced_requests_total %d\n", queued, active, merged)
			fmt.Fprintf(w, "flamedb_postgres_connections %d\nflamedb_postgres_connections_acquired %d\nflamedb_postgres_connections_idle %d\nflamedb_postgres_acquire_total %d\nflamedb_postgres_acquire_wait_seconds_total %g\n", pool.TotalConns(), pool.AcquiredConns(), pool.IdleConns(), pool.AcquireCount(), pool.AcquireDuration().Seconds())
		})
		monitor = &http.Server{Addr: cfg.Server.MetricsAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
		go func() {
			err := monitor.ListenAndServe()
			if !errors.Is(err, http.ErrServerClosed) {
				failures <- err
			}
		}()
	}
	addr := net.JoinHostPort(cfg.Server.Host, fmt.Sprint(cfg.Server.Port))
	go func() { failures <- srv.Listen(addr) }()
	logger.Info("shared storage server started", "address", addr, "engine", cfg.Storage.Engine)
	var result error
	select {
	case <-ctx.Done():
	case result = <-failures:
		if errors.Is(result, net.ErrClosed) {
			result = nil
		}
	}
	logger.Info("draining server")
	srv.Close()
	if monitor != nil {
		shutdown, done := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer done()
		_ = monitor.Shutdown(shutdown)
	}
	if profiler != nil {
		shutdown, done := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer done()
		_ = profiler.Shutdown(shutdown)
	}
	return result
}
