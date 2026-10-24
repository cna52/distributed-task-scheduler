package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"time"
)

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
func envInt(name string, fallback int) int {
	v, e := strconv.Atoi(os.Getenv(name))
	if e != nil || v < 1 {
		return fallback
	}
	return v
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	if cmd == "submit" || cmd == "status" || cmd == "bench" {
		if err := clientCommand(cmd, os.Args[2:]); err != nil {
			logger.Error("command failed", "error", err)
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	s, err := openStore(ctx, env("DATABASE_URL", "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"))
	if err != nil {
		logger.Error("postgres connect", "error", err)
		os.Exit(1)
	}
	defer s.DB.Close()
	if cmd == "migrate" {
		if err := s.migrate(ctx); err != nil {
			logger.Error("migrate", "error", err)
			os.Exit(1)
		}
		logger.Info("migrations applied")
		return
	}
	if cmd != "serve" && cmd != "worker" && cmd != "dispatch" {
		usage()
		os.Exit(2)
	}
	q, err := openQueue(ctx, env("REDIS_URL", "redis://localhost:6379/0"))
	if err != nil {
		logger.Error("redis connect", "error", err)
		os.Exit(1)
	}
	defer q.Client.Close()
	switch cmd {
	case "serve":
		api := &API{Store: s, Queue: q, Logger: logger}
		server := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: api.routes(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			<-ctx.Done()
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server.Shutdown(c)
		}()
		logger.Info("api listening", "address", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("api", "error", err)
			os.Exit(1)
		}
	case "worker":
		lease := time.Duration(envInt("LEASE_SECONDS", 30)) * time.Second
		w := &Worker{Store: s, Queue: q, Concurrency: envInt("WORKER_CONCURRENCY", 4), Lease: lease, Logger: logger}
		logger.Info("worker started", "concurrency", w.Concurrency, "lease", lease.String())
		w.run(ctx)
	case "dispatch":
		logger.Info("dispatcher started")
		(&Dispatcher{Store: s, Queue: q, Logger: logger}).run(ctx)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: scheduler {migrate|serve|worker|dispatch|submit|status|bench}")
}

func clientCommand(cmd string, args []string) error {
	base := env("API_URL", "http://localhost:8080")
	client := &http.Client{Timeout: 15 * time.Second}
	switch cmd {
	case "submit":
		fs := flag.NewFlagSet("submit", flag.ContinueOnError)
		typ := fs.String("type", "sleep", "registered handler")
		payload := fs.String("payload", `{"duration_ms":0}`, "JSON object")
		priority := fs.String("priority", "normal", "high, normal, low")
		max := fs.Int("max-attempts", 3, "max attempts")
		key := fs.String("key", "", "idempotency key")
		delay := fs.Duration("delay", 0, "delay before execution")
		if err := fs.Parse(args); err != nil {
			return err
		}
		r := SubmitRequest{Type: *typ, Payload: json.RawMessage(*payload), Priority: *priority, MaxAttempts: max, IdempotencyKey: *key}
		if *delay > 0 {
			t := time.Now().Add(*delay)
			r.RunAt = &t
		}
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		resp, err := client.Post(base+"/jobs", "application/json", bytes.NewReader(b))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Print(string(body))
		if resp.StatusCode >= 300 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		return nil
	case "status":
		if len(args) != 1 {
			return fmt.Errorf("status requires a job ID")
		}
		resp, err := client.Get(base + "/jobs/" + args[0])
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Print(string(body))
		if resp.StatusCode >= 300 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		return nil
	case "bench":
		return benchmark(base, args)
	}
	return fmt.Errorf("unknown command")
}

func benchmark(base string, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	n := fs.Int("jobs", 1000, "job count")
	concurrency := fs.Int("concurrency", 20, "parallel submissions")
	durationMS := fs.Int("duration-ms", 0, "sleep handler duration in milliseconds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *n < 1 || *concurrency < 1 || *durationMS < 0 || *durationMS > 300000 {
		return fmt.Errorf("jobs and concurrency must be positive; duration-ms must be 0..300000")
	}
	client := &http.Client{Timeout: 15 * time.Second}
	type sent struct {
		id    string
		start time.Time
	}
	jobs := make([]sent, *n)
	work := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	started := time.Now()
	requestBody := []byte(fmt.Sprintf(`{"type":"sleep","payload":{"duration_ms":%d}}`, *durationMS))
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range work {
				jobs[k].start = time.Now()
				resp, err := client.Post(base+"/jobs", "application/json", bytes.NewReader(requestBody))
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					continue
				}
				var j Job
				err = json.NewDecoder(resp.Body).Decode(&j)
				resp.Body.Close()
				if err != nil || resp.StatusCode != http.StatusAccepted {
					select {
					case errCh <- fmt.Errorf("submit status %d: %v", resp.StatusCode, err):
					default:
					}
					continue
				}
				jobs[k].id = j.ID
			}
		}()
	}
	for k := range jobs {
		work <- k
	}
	close(work)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	latencies := make([]time.Duration, *n)
	remaining := *n
	lastFinish := started
	deadline := time.Now().Add(5 * time.Minute)
	for remaining > 0 && time.Now().Before(deadline) {
		for i, j := range jobs {
			if latencies[i] != 0 {
				continue
			}
			resp, err := client.Get(base + "/jobs/" + j.id)
			if err != nil {
				return err
			}
			var state Job
			err = json.NewDecoder(resp.Body).Decode(&state)
			resp.Body.Close()
			if err != nil {
				return err
			}
			if state.Status == "failed" {
				return fmt.Errorf("job %s failed", j.id)
			}
			if state.Status == "succeeded" {
				if state.FinishedAt == nil {
					return fmt.Errorf("job %s has no finished_at", j.id)
				}
				latencies[i] = state.FinishedAt.Sub(j.start)
				if state.FinishedAt.After(lastFinish) {
					lastFinish = *state.FinishedAt
				}
				remaining--
			}
		}
		if remaining > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if remaining > 0 {
		return fmt.Errorf("timed out with %d unfinished jobs", remaining)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	elapsed := lastFinish.Sub(started)
	fmt.Printf("jobs=%d concurrency=%d duration_ms=%d elapsed=%s throughput=%.2f jobs/s p95=%s\n", *n, *concurrency, *durationMS, elapsed, float64(*n)/elapsed.Seconds(), latencies[(95*(*n)-1)/100])
	return nil
}
