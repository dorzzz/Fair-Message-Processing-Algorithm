package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/doradiv/fmpa/policy"
)

/*
Read the instruction.txt before running this nats example
*/

func main() {
	cfg := parseFlags()

	p := policy.NewPolicy(CAPACITY)
	// Explicit quotas per instructions (avoid relying on auto-add)
	p.AddTenant("tenant1", 1)
	p.AddTenant("tenant2", 2)
	p.AddTenant("tenant3", 1)
	p.AddTenant("tenant4", 3)
	p.AddTenant("tenant5", 1)

	// Graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if cfg.JetStream {
		if cfg.Reset {
			if err := resetJetStream(cfg.NatsURL, cfg.Stream, cfg.Durable); err != nil {
				log.Printf("reset failed: %v", err)
				os.Exit(1)
			}
		}
		if err := runJetStream(ctx, p, cfg.NatsURL, cfg.Stream, cfg.Durable, cfg.LogEach, cfg.DeliverNew); err != nil {
			log.Printf("JetStream error: %v", err)
			os.Exit(1)
		}
		return
	}

	log.Println("no mode selected; use --jetstream")
}
