// Command router runs one role of the event router: the ingester, a worker or the monitor.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"bskyrouter/internal/broker"
	"bskyrouter/internal/config"
	"bskyrouter/internal/handlers"
	"bskyrouter/internal/ingest"
	"bskyrouter/internal/router"
	"bskyrouter/internal/worker"
)

func main() {
	role := flag.String("role", "", "setup | ingester | worker")
	pathType := flag.String("type", "", "worker: posts | likes | deletes")
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	dryRun := flag.Bool("dry-run", false, "ingester: route and count events without publishing")
	checkConfig := flag.Bool("check-config", false, "load and validate the config, print it, and exit")
	flag.Parse()

	if *checkConfig {
		cfg, err := config.Load(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Print(cfg.Summary())
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("role", *role)

	if err := run(*role, *pathType, *configPath, *dryRun, log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(role, pathType, configPath string, dryRun bool, log *slog.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch role {
	case "setup":
		return setup(ctx, cfg, log)
	case "ingester":
		return runIngester(ctx, cfg, dryRun, log)
	case "worker":
		return runWorker(ctx, cfg, pathType, log.With("type", pathType))
	default:
		return fmt.Errorf("unknown --role %q", role)
	}
}

func runIngester(ctx context.Context, cfg config.Config, dryRun bool, log *slog.Logger) error {
	in := &ingest.Ingester{
		URL:            cfg.Jetstream.URL,
		Routes:         router.Table,
		MaxPending:     cfg.Jetstream.MaxPending,
		CursorInterval: cfg.Jetstream.CursorInterval,
		Backoff:        cfg.Jetstream.ReconnectBackoff,
		GapThreshold:   cfg.Jetstream.GapThreshold,
		DryRun:         dryRun,
		Log:            log,
	}
	if !dryRun {
		nc, js, err := broker.ConnectPublisher(cfg.NATS.URL, cfg.Jetstream.MaxPending, cfg.Jetstream.PublishTimeout)
		if err != nil {
			return err
		}
		defer nc.Close()
		if err := broker.Ensure(ctx, js, cfg); err != nil {
			return err
		}
		in.JS = js
		cursor, err := broker.NewCursorStore(ctx, js, "ingester")
		if err != nil {
			return err
		}
		in.Cursor = cursor
	}
	return in.Run(ctx)
}

func runWorker(ctx context.Context, cfg config.Config, pathType string, log *slog.Logger) error {
	nc, js, err := broker.Connect(cfg.NATS.URL, "worker-"+pathType)
	if err != nil {
		return err
	}
	defer nc.Close()
	if err := broker.Ensure(ctx, js, cfg); err != nil {
		return err
	}

	w := &worker.Worker{
		Name:       pathType,
		JS:         js,
		AckWait:    cfg.Workers.AckWait,
		MaxDeliver: cfg.Workers.MaxDeliver,
		Backoff:    cfg.Workers.Backoff,
		Log:        log,
	}
	switch pathType {
	case "deletes":
		tombstones, err := broker.NewTombstones(ctx, js)
		if err != nil {
			return err
		}
		w.Stream, w.ProcessDelay = broker.StreamDeletes, cfg.Workers.Deletes.ProcessDelay
		w.Handler = &handlers.Deletes{Tombstones: tombstones}
	default:
		return fmt.Errorf("unknown or not yet built --type %q", pathType)
	}
	return w.Run(ctx)
}

// setup creates the streams and KV buckets and exits. Every role also does
// this at startup; this role exists to do it on its own.
func setup(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	nc, js, err := broker.Connect(cfg.NATS.URL, "setup")
	if err != nil {
		return err
	}
	defer nc.Close()
	if err := broker.Ensure(ctx, js, cfg); err != nil {
		return err
	}
	log.Info("streams and buckets ready", "nats", cfg.NATS.URL)
	return nil
}
