package main

import (
	"flag"

	"github.com/nats-io/nats.go"
)

// Config holds CLI configuration for the app.
type Config struct {
	JetStream  bool
	NatsURL    string
	Stream     string
	Durable    string
	Reset      bool
	DeliverNew bool
	LogEach    bool
}

// parseFlags reads command-line flags and returns a Config.
func parseFlags() Config {
	var cfg Config
	flag.BoolVar(&cfg.JetStream, "jetstream", false, "enable JetStream pull consumer mode")
	flag.StringVar(&cfg.NatsURL, "nats_url", nats.DefaultURL, "NATS server URL")
	flag.StringVar(&cfg.Stream, "stream", "MESSAGES", "JetStream stream name")
	flag.StringVar(&cfg.Durable, "durable", "workers", "JetStream durable consumer name")
	flag.BoolVar(&cfg.Reset, "reset", false, "purge stream and delete durable before starting")
	flag.BoolVar(&cfg.DeliverNew, "deliver_new", false, "start consumer at new messages instead of all")
	flag.BoolVar(&cfg.LogEach, "log_each", false, "log each processed message (subject, tenant, sequences)")
	flag.Parse()
	return cfg
}
