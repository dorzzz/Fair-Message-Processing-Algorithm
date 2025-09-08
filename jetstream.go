package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doradiv/fmpa/policy"
	"github.com/nats-io/nats.go"
)

const CAPACITY int = 100

// process simulates message handling work.
func process(data []byte) error {
	time.Sleep(300 * time.Millisecond) // visible concurrency
	return nil
}

// ensureStream creates a stream for the known message types if it does not exist.
func ensureStream(js nats.JetStreamContext, stream string, subjects []string) error {
	info, err := js.StreamInfo(stream)
	if err == nil && info != nil {
		// ensure subjects contain required set
		required := map[string]struct{}{}
		for _, s := range subjects {
			required[s] = struct{}{}
		}
		have := map[string]struct{}{}
		for _, s := range info.Config.Subjects {
			have[s] = struct{}{}
		}
		update := false
		for s := range required {
			if _, ok := have[s]; !ok {
				info.Config.Subjects = append(info.Config.Subjects, s)
				update = true
			}
		}
		if update {
			_, err = js.UpdateStream(&info.Config)
			return err
		}
		return nil
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: subjects,
		Storage:  nats.FileStorage,
		Replicas: 1,
	})
	return err
}

// ensureConsumer creates a durable pull consumer bound to the stream if it does not exist.
func ensureConsumer(js nats.JetStreamContext, stream, durable string, deliverNew bool) error {
	if _, err := js.ConsumerInfo(stream, durable); err == nil {
		return nil // exists
	}
	dp := nats.DeliverAllPolicy
	if deliverNew {
		dp = nats.DeliverNewPolicy
	}
	_, err := js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: dp,
		FilterSubject: "", // receive all stream subjects
		AckWait:       30 * time.Second,
		MaxAckPending: 1024,
		ReplayPolicy:  nats.ReplayInstantPolicy,
	})
	return err
}

// printFinalSummary prints sorted per-tenant counts and expected vs actual shares.
func printFinalSummary(p *policy.MessageConsumptionPolicy, processed map[string]int, totalProcessed uint64) {
	// Sorted per-tenant summary
	keys := make([]string, 0, len(processed))
	for k := range processed {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		fmt.Print("Summary:")
		for _, k := range keys {
			fmt.Printf(" %s=%d", k, processed[k])
		}
		fmt.Println()
	}

	// Expected vs actual shares based on ProcessingSets
	stats := p.GetStats()
	var totalSets int
	for _, v := range stats {
		if m, ok := v.(map[string]int); ok {
			totalSets += m["sets"]
		}
	}
	fmt.Printf("Total processed: %d\n", totalProcessed)
	if totalSets > 0 && len(stats) > 0 {
		type row struct {
			id          string
			sets        int
			processed   int
			expectedPct float64
			actualPct   float64
		}
		var rows []row
		for id, v := range stats {
			m := v.(map[string]int)
			sets := m["sets"]
			done := processed[id]
			exp := float64(sets) / float64(totalSets) * 100.0
			act := 0.0
			if totalProcessed > 0 {
				act = float64(done) / float64(totalProcessed) * 100.0
			}
			rows = append(rows, row{id: id, sets: sets, processed: done, expectedPct: exp, actualPct: act})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
		fmt.Println("Share by tenant (ProcessingSets vs processed):")
		for _, r := range rows {
			fmt.Printf("  %s: sets=%d expected≈%.1f%% actual≈%.1f%% processed=%d\n", r.id, r.sets, r.expectedPct, r.actualPct, r.processed)
		}
	}
}

// resetJetStream deletes the durable and purges the stream if they exist.
func resetJetStream(natsURL, stream, durable string) error {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("reset connect NATS: %w", err)
	}
	defer nc.Drain()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("reset jetstream: %w", err)
	}
	// best-effort delete consumer
	if _, err := js.ConsumerInfo(stream, durable); err == nil {
		_ = js.DeleteConsumer(stream, durable)
	}
	// best-effort purge stream
	if _, err := js.StreamInfo(stream); err == nil {
		if err := js.PurgeStream(stream); err != nil {
			log.Printf("reset purge stream error: %v", err)
		}
	}
	return nil
}

func runJetStream(ctx context.Context, p *policy.MessageConsumptionPolicy, url, stream, durable string, logEach bool, deliverNew bool) error {
	nc, err := nats.Connect(url)
	if err != nil {
		return fmt.Errorf("connect NATS: %w", err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream ctx: %w", err)
	}

	// Configure subjects per the instructions
	subjects := []string{
		"order.*",
		"payment.*",
		"notification.*",
		"analytics.*",
	}

	if err := ensureStream(js, stream, subjects); err != nil {
		return fmt.Errorf("ensure stream: %w", err)
	}
	if err := ensureConsumer(js, stream, durable, deliverNew); err != nil {
		return fmt.Errorf("ensure consumer: %w", err)
	}

	// Bind to the durable pull consumer created above; no subject needed when binding.
	sub, err := js.PullSubscribe("", durable, nats.Bind(stream, durable), nats.ManualAck())
	if err != nil {
		// if binding failed due to not found, surface clearer error
		return fmt.Errorf("pull subscribe bind failed: %w", err)
	}
	if ci, err := js.ConsumerInfo(stream, durable); err == nil {
		log.Printf("consumer bound: pending=%d ack_pending=%d waiting=%d delivered(c=%d s=%d)",
			ci.NumPending, ci.NumAckPending, ci.NumWaiting, ci.Delivered.Consumer, ci.Delivered.Stream)
		if ci.Config.FilterSubject != "" {
			return fmt.Errorf("consumer %q has unexpected filter %q; delete it or use --reset", durable, ci.Config.FilterSubject)
		}
		if deliverNew && ci.Config.DeliverPolicy != nats.DeliverNewPolicy {
			return fmt.Errorf("consumer %q not set to DeliverNew; use --reset to recreate with --deliver_new", durable)
		}
	}

	var mu sync.Mutex
	processed := make(map[string]int)
	var totalProcessed uint64

	sem := make(chan struct{}, CAPACITY)
	var inFlight int32

	lastActivity := time.Now()
	lastFetchCount := -1

	idle := false

	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown requested")
			return nil
		default:
		}

		msgs, err := sub.Fetch(CAPACITY, nats.MaxWait(500*time.Millisecond))
		if err != nil && err != nats.ErrTimeout {
			log.Printf("fetch error: %v", err)
			continue
		}
		if len(msgs) == 0 {
			// No new messages fetched; if also no in-flight work and the consumer has no backlog, declare finished.
			if atomic.LoadInt32(&inFlight) == 0 && time.Since(lastActivity) > 2*time.Second {
				if ci, err := js.ConsumerInfo(stream, durable); err == nil {
					if ci.NumPending == 0 && ci.NumAckPending == 0 {
						if !idle {
							log.Println("Finished processing all messages")
							// Snapshot processed counts under lock, then print outside.
							mu.Lock()
							snap := make(map[string]int, len(processed))
							for k, v := range processed {
								snap[k] = v
							}
							mu.Unlock()
							printFinalSummary(p, snap, atomic.LoadUint64(&totalProcessed))
							idle = true
						}
					}
				}
			}
			continue
		}

		// Got messages again; mark activity and announce once if we were idle.
		lastActivity = time.Now()
		if idle {
			log.Println("New messages received")
			idle = false
		}

		// randomize order within the batch to reduce ordering bias
		rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })

		// Quieter fetch logging: only log when the batch size changes or is a full batch
		if len(msgs) != lastFetchCount || len(msgs) == CAPACITY {
			log.Printf("fetched %d messages", len(msgs))
			lastFetchCount = len(msgs)
		}

		// Drain this fetched batch fairly: retry locally before NAKing to avoid redelivery loops.
		remaining := msgs
		for pass := 0; pass < 50 && len(remaining) > 0; pass++ {
			var deferred []*nats.Msg
			progress := 0

			for _, msg := range remaining {
				// subject format: {messageType}.{tenantId}
				parts := strings.Split(msg.Subject, ".")
				if len(parts) != 2 {
					_ = msg.Term() // malformed subject, don't redeliver
					continue
				}
				msgType, tenantID := parts[0], parts[1]

				// If tenant not known yet, add with default 1 set. Do not overwrite existing quotas.
				if _, ok := p.GetStats()[tenantID]; !ok {
					p.AddTenant(tenantID, 1)
				}

				// Check policy; if not allowed right now, defer within this batch.
				if !p.ShouldConsume(tenantID, msgType) {
					_ = msg.Nak()
					continue
				}

				// Allowed: start work under global capacity.
				sem <- struct{}{}
				p.OnMessageStart(tenantID, msgType)
				atomic.AddInt32(&inFlight, 1)
				progress++

				go func(m *nats.Msg, tID, mType string) {
					defer func() {
						p.OnMessageComplete(tID, mType)
						atomic.AddInt32(&inFlight, -1)
						<-sem
					}()

					if err := process(m.Data); err != nil {
						log.Printf("handler error for %s: %v", m.Subject, err)
						_ = m.Nak()
						return
					}

					mu.Lock()
					processed[tID]++
					mu.Unlock()
					atomic.AddUint64(&totalProcessed, 1)
					_ = m.Ack()
					if logEach {
						if md, err := m.Metadata(); err == nil {
							log.Printf("ACK subject=%s tenant=%s sseq=%d cseq=%d", m.Subject, tID, md.Sequence.Stream, md.Sequence.Consumer)
						} else {
							log.Printf("ACK subject=%s tenant=%s", m.Subject, tID)
						}
					}
					lastActivity = time.Now()
				}(msg, tenantID, msgType)
			}

			// If we made no progress this pass, stop retrying locally.
			if progress == 0 {
				remaining = deferred
				break
			}

			// Prepare next pass with only deferred msgs, small pause to allow completions to free per-tenant slots.
			remaining = deferred
			if len(remaining) > 0 {
				time.Sleep(50 * time.Millisecond)
			}
		}
		// Any messages still remaining after retries: NAK with a small delay so they reappear later.
		for _, msg := range remaining {
			_ = msg.Nak() // immediate requeue keeps them in pending so we don't declare finished early
		}
	}
}
