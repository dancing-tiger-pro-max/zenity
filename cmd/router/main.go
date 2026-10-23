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

	"bskyrouter/internal/config"
	"bskyrouter/internal/ingest"
	"bskyrouter/internal/router"
)

func main() {
	role := flag.String("role", "", "ingester")
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	dryRun := flag.Bool("dry-run", false, "ingester: route and count events without publishing")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("role", *role)

	if err := run(*role, *configPath, *dryRun, log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(role, configPath string, dryRun bool, log *slog.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch role {
	case "ingester":
		if !dryRun {
			return fmt.Errorf("publishing to NATS isn't built yet; run with --dry-run")
		}
		in := &ingest.Ingester{URL: cfg.Jetstream.URL, Routes: router.Table, DryRun: dryRun, Log: log}
		return in.Run(ctx)
	default:
		return fmt.Errorf("unknown --role %q", role)
	}
}
