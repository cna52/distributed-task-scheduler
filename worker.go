package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type Worker struct {
	Store       *Store
	Queue       *Queue
	Concurrency int
	Lease       time.Duration
	Logger      *slog.Logger
}

func (w *Worker) run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < w.Concurrency; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w.loop(ctx) }()
	}
	wg.Wait()
}

func (w *Worker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		id, err := w.Queue.pop(ctx)
		if err != nil {
			w.Logger.Error("queue pop", "error", err)
			idleWait(ctx, time.Second)
			continue
		}
		if id == "" {
			idleWait(ctx, 200*time.Millisecond)
			continue
		}
		j, err := w.Store.claim(ctx, id, w.Lease)
		if err == pgx.ErrNoRows {
			continue
		}
		if err != nil {
			w.Logger.Error("claim", "job_id", id, "error", err)
			idleWait(ctx, time.Second)
			continue
		}
		w.process(ctx, j)
	}
}

func (w *Worker) process(parent context.Context, j Job) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(w.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				hctx, stop := context.WithTimeout(context.Background(), w.Lease/3)
				ok, err := w.Store.heartbeat(hctx, j.ID, *j.LeaseToken, w.Lease)
				stop()
				if err != nil || !ok {
					w.Logger.Warn("lease renewal failed", "job_id", j.ID, "error", err)
					cancel()
					return
				}
			}
		}
	}()
	result, err := execute(ctx, j)
	cancel()
	<-done
	if parent.Err() != nil {
		return
	}
	commitCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err == nil {
		ok, e := w.Store.complete(commitCtx, j, result)
		if e != nil {
			w.Logger.Error("complete", "job_id", j.ID, "error", e)
		} else if !ok {
			w.Logger.Warn("stale completion rejected", "job_id", j.ID)
		}
		return
	}
	// Jitter spreads retries from a shared dependency outage.
	delay := retryDelay(j.Attempts)
	delay += time.Duration(rand.Int63n(int64(delay/4) + 1))
	ok, e := w.Store.fail(commitCtx, j, err.Error(), delay)
	if e != nil {
		w.Logger.Error("fail", "job_id", j.ID, "error", e)
	} else if !ok {
		w.Logger.Warn("stale failure rejected", "job_id", j.ID)
	}
}

func execute(ctx context.Context, j Job) ([]byte, error) {
	switch j.Type {
	case "sleep":
		var p struct {
			DurationMS int `json:"duration_ms"`
		}
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return nil, err
		}
		if p.DurationMS < 0 || p.DurationMS > 300000 {
			return nil, errors.New("invalid sleep duration")
		}
		t := time.NewTimer(time.Duration(p.DurationMS) * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
		}
		return []byte(`{"slept":true}`), nil
	case "fail":
		var p struct {
			Failures int `json:"failures"`
		}
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return nil, err
		}
		if j.Attempts <= p.Failures {
			return nil, fmt.Errorf("planned failure on attempt %d", j.Attempts)
		}
		return []byte(`{"recovered":true}`), nil
	case "record_event":
		// The event insert happens with completion in a single fenced DB transaction.
		return []byte(`{"recorded":true}`), nil
	default:
		return nil, fmt.Errorf("unknown job type %q", j.Type)
	}
}

type Dispatcher struct {
	Store  *Store
	Queue  *Queue
	Logger *slog.Logger
}

func (d *Dispatcher) run(ctx context.Context) {
	lastSweep := time.Time{}
	for ctx.Err() == nil {
		if time.Since(lastSweep) >= time.Second {
			if n, err := d.Store.expireLeases(ctx, 100); err != nil {
				d.Logger.Error("expire leases", "error", err)
			} else if n > 0 {
				d.Logger.Info("expired leases", "count", n)
			}
			if _, err := d.Store.reconcile(ctx, d.Queue, 1000); err != nil {
				d.Logger.Error("reconcile queue", "error", err)
			}
			lastSweep = time.Now()
		}
		n, err := d.Store.publishOutbox(ctx, d.Queue, 200)
		if err != nil {
			d.Logger.Error("publish outbox", "error", err)
		}
		if n == 200 && err == nil {
			continue
		}
		idleWait(ctx, 100*time.Millisecond)
	}
}
