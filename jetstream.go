package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
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

// publishSynthetic publishes N messages across tenants 1..5 on subjects order.tenant{N}.
func publishSynthetic(js nats.JetStreamContext, total int) error {
	for i := 0; i < total; i++ {
		t := (i % 5) + 1
		subj := "order.tenant" + strconv.Itoa(t)
		if _, err := js.Publish(subj, []byte("{}")); err != nil {
			return err
		}
	}
	return nil
}

// printFinalSummary prints sorted per-tenant counts and expected vs actual shares.
func printFinalSummary(p *policy.MessageConsumptionPolicy, processed map[string]int, totalProcessed uint64, producedThisRun uint64, deliveredThisRun uint64) {
	fmt.Printf("Produced this run: %d\n", producedThisRun)
	fmt.Printf("Delivered to this consumer this run: %d\n", deliveredThisRun)

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

	// Bind to the durable pull consumer. Use the exact filter subject if the durable has one.
	var bindSubject string
	if ciProbe, err := js.ConsumerInfo(stream, durable); err == nil && ciProbe != nil {
		bindSubject = ciProbe.Config.FilterSubject
	}
	subjectForBind := bindSubject // empty means "use durable's filter"
	sub, err := js.PullSubscribe(subjectForBind, durable, nats.Bind(stream, durable), nats.ManualAck())
	if err != nil {
		return fmt.Errorf("pull subscribe bind failed: %w (filter=%q)", err, subjectForBind)
	}
	if ci, err := js.ConsumerInfo(stream, durable); err == nil {
		log.Printf("consumer bound: pending=%d ack_pending=%d waiting=%d delivered(c=%d s=%d) filter=%q",
			ci.NumPending, ci.NumAckPending, ci.NumWaiting, ci.Delivered.Consumer, ci.Delivered.Stream, ci.Config.FilterSubject)
		if deliverNew && ci.Config.DeliverPolicy != nats.DeliverNewPolicy {
			return fmt.Errorf("consumer %q not set to DeliverNew; use --reset to recreate with --deliver_new", durable)
		}
	}

	// Declare baseline variables for use in select loop and later
	var deliveredStart uint64
	var msgsStart uint64
	if ci, err := js.ConsumerInfo(stream, durable); err == nil && ci != nil {
		deliveredStart = ci.Delivered.Consumer
	}
	if si0, err := js.StreamInfo(stream); err == nil && si0 != nil {
		msgsStart = si0.State.Msgs
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
			// Final summary on exit path
			mu.Lock()
			snap := make(map[string]int, len(processed))
			for k, v := range processed {
				snap[k] = v
			}
			mu.Unlock()

			var producedNow, deliveredNow uint64
			if siNow, err := js.StreamInfo(stream); err == nil && siNow != nil {
				producedNow = siNow.State.Msgs - msgsStart
			}
			if ciNow, err := js.ConsumerInfo(stream, durable); err == nil && ciNow != nil {
				deliveredNow = ciNow.Delivered.Consumer - deliveredStart
			}
			printFinalSummary(p, snap, atomic.LoadUint64(&totalProcessed), producedNow, deliveredNow)
			return nil
		default:
		}

		msgs, err := sub.Fetch(CAPACITY, nats.MaxWait(5*time.Second))
		if err != nil && err != nats.ErrTimeout {
			log.Printf("fetch error: %v", err)
			continue
		}
		if len(msgs) == 0 {
			// No new messages fetched; if also no in-flight work and the consumer has no backlog, declare finished.
			if atomic.LoadInt32(&inFlight) == 0 && time.Since(lastActivity) > 2*time.Second {
				if ci, err := js.ConsumerInfo(stream, durable); err == nil {
					log.Printf("idle poll: pending=%d ack_pending=%d waiting=%d delivered(c=%d s=%d)",
						ci.NumPending, ci.NumAckPending, ci.NumWaiting, ci.Delivered.Consumer, ci.Delivered.Stream)
					if ci.NumPending == 0 && ci.NumAckPending == 0 && atomic.LoadInt32(&inFlight) == 0 {
						if !idle {
							log.Println("Finished processing all messages")
							log.Printf("consumer totals: delivered=%d pending=%d ack_pending=%d waiting=%d", ci.Delivered.Consumer, ci.NumPending, ci.NumAckPending, ci.NumWaiting)
							// Snapshot processed counts under lock, then print outside.
							mu.Lock()
							snap := make(map[string]int, len(processed))
							for k, v := range processed {
								snap[k] = v
							}
							mu.Unlock()

							var producedNow, deliveredNow uint64
							if siNow, err := js.StreamInfo(stream); err == nil && siNow != nil {
								producedNow = siNow.State.Msgs - msgsStart
							}
							if ciNow, err := js.ConsumerInfo(stream, durable); err == nil && ciNow != nil {
								deliveredNow = ciNow.Delivered.Consumer - deliveredStart
							}
							printFinalSummary(p, snap, atomic.LoadUint64(&totalProcessed), producedNow, deliveredNow)
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
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		rng.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })

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
					// Defer locally within this batch to give other tenants/messages a chance this pass.
					deferred = append(deferred, msg)
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

					if err := m.AckSync(); err != nil {
						log.Printf("ack failed for %s: %v", m.Subject, err)
						// Do not count as processed on ack failure
						return
					}

					mu.Lock()
					processed[tID]++
					mu.Unlock()
					atomic.AddUint64(&totalProcessed, 1)
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
		// NAK any still-deferred messages after all local retries so they can reappear later via redelivery.
		for _, msg := range remaining {
			_ = msg.Nak() // immediate requeue keeps them in pending so we don't declare finished early
		}
	}
	// Final summary on exit path
	mu.Lock()
	snap := make(map[string]int, len(processed))
	for k, v := range processed {
		snap[k] = v
	}
	mu.Unlock()

	var producedNow, deliveredNow uint64
	if siNow, err := js.StreamInfo(stream); err == nil && siNow != nil {
		producedNow = siNow.State.Msgs - msgsStart
	}
	if ciNow, err := js.ConsumerInfo(stream, durable); err == nil && ciNow != nil {
		deliveredNow = ciNow.Delivered.Consumer - deliveredStart
	}
	printFinalSummary(p, snap, atomic.LoadUint64(&totalProcessed), producedNow, deliveredNow)

	return nil
}
