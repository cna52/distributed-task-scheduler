package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

func testServices(t *testing.T) (*Store, *Queue) {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	redisURL := os.Getenv("TEST_REDIS_URL")
	if dbURL == "" || redisURL == "" {
		t.Skip("set TEST_DATABASE_URL and TEST_REDIS_URL for integration tests")
	}
	ctx := context.Background()
	s, err := openStore(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	q, err := openQueue(ctx, redisURL)
	if err != nil {
		s.DB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Client.Close(); s.DB.Close() })
	if err := s.migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ctx, "TRUNCATE outbox,recorded_events,jobs RESTART IDENTITY CASCADE"); err != nil {
		t.Fatal(err)
	}
	if err := q.Client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return s, q
}

func request(typ, payload string) SubmitRequest {
	r := SubmitRequest{Type: typ, Payload: json.RawMessage(payload)}
	if err := r.normalize(); err != nil {
		panic(err)
	}
	return r
}

func waitStatus(t *testing.T, s *Store, id, status string) Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		j, err := s.getJob(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == status {
			return j
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s", id, status)
	return Job{}
}

func TestSubmitIdempotencyAndOutbox(t *testing.T) {
	s, q := testServices(t)
	ctx := context.Background()
	r := request("sleep", `{"duration_ms":0}`)
	r.IdempotencyKey = "same-key"
	a, existing, err := s.submit(ctx, r)
	if err != nil || existing {
		t.Fatalf("first submit: %v %v", err, existing)
	}
	b, existing, err := s.submit(ctx, r)
	if err != nil || !existing || a.ID != b.ID {
		t.Fatalf("repeat submit: %v %v", err, existing)
	}
	r.Payload = json.RawMessage(`{"duration_ms":1}`)
	_, _, err = s.submit(ctx, r)
	if !errors.Is(err, errConflict) {
		t.Fatalf("wanted conflict, got %v", err)
	}
	n, err := s.publishOutbox(ctx, q, 10)
	if err != nil || n != 1 {
		t.Fatalf("publish: %d %v", n, err)
	}
	id, err := q.pop(ctx)
	if err != nil || id != a.ID {
		t.Fatalf("pop: %s %v", id, err)
	}
}

func TestConcurrentClaimAndStaleLease(t *testing.T) {
	s, q := testServices(t)
	ctx := context.Background()
	j, _, err := s.submit(ctx, request("sleep", `{"duration_ms":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.publishOutbox(ctx, q, 10); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan Job, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x, e := s.claim(ctx, j.ID, 150*time.Millisecond)
			if e == nil {
				results <- x
			} else if e != pgx.ErrNoRows {
				t.Errorf("claim: %v", e)
			}
		}()
	}
	wg.Wait()
	close(results)
	var owner Job
	count := 0
	for x := range results {
		owner = x
		count++
	}
	if count != 1 {
		t.Fatalf("active claims=%d", count)
	}
	time.Sleep(180 * time.Millisecond)
	ok, err := s.complete(ctx, owner, []byte(`{}`))
	if err != nil || ok {
		t.Fatalf("stale completion accepted: %v %v", ok, err)
	}
	n, err := s.expireLeases(ctx, 10)
	if err != nil || n != 1 {
		t.Fatalf("expire: %d %v", n, err)
	}
	x, err := s.getJob(ctx, j.ID)
	if err != nil || x.Status != "queued" {
		t.Fatalf("requeue: %+v %v", x, err)
	}
}

func TestLostPopReconcilesAndRetryCompletes(t *testing.T) {
	s, q := testServices(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := request("fail", `{"failures":1}`)
	two := 2
	r.MaxAttempts = &two
	j, _, err := s.submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.publishOutbox(ctx, q, 10); err != nil {
		t.Fatal(err)
	}
	if _, err = q.pop(ctx); err != nil {
		t.Fatal(err)
	} // crash before PostgreSQL claim
	if _, err = s.DB.Exec(ctx, `UPDATE jobs SET enqueued_at=now()-interval '31 seconds' WHERE id=$1`, j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.reconcile(ctx, q, 10); err != nil {
		t.Fatal(err)
	}
	w := &Worker{Store: s, Queue: q, Concurrency: 2, Lease: time.Second, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	d := &Dispatcher{Store: s, Queue: q, Logger: w.Logger}
	go w.run(ctx)
	go d.run(ctx)
	finished := waitStatus(t, s, j.ID, "succeeded")
	if finished.Attempts != 2 {
		t.Fatalf("attempts=%d", finished.Attempts)
	}
}

func TestEventSideEffectIsUnique(t *testing.T) {
	s, q := testServices(t)
	ctx := context.Background()
	j, _, err := s.submit(ctx, request("record_event", `{"name":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.publishOutbox(ctx, q, 10); err != nil {
		t.Fatal(err)
	}
	owner, err := s.claim(ctx, j.ID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := s.complete(ctx, owner, []byte(`{"recorded":true}`))
	if err != nil || !ok {
		t.Fatalf("complete: %v %v", ok, err)
	}
	ok, err = s.complete(ctx, owner, []byte(`{"recorded":true}`))
	if err != nil || ok {
		t.Fatalf("duplicate complete: %v %v", ok, err)
	}
	var n int
	if err = s.DB.QueryRow(ctx, `SELECT count(*) FROM recorded_events WHERE job_id=$1`, j.ID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("events=%d %v", n, err)
	}
}

func TestDelayedJobAndExhaustedAttempts(t *testing.T) {
	s, q := testServices(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := request("fail", `{"failures":20}`)
	one := 1
	r.MaxAttempts = &one
	future := time.Now().Add(200 * time.Millisecond)
	r.RunAt = &future
	j, _, err := s.submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.publishOutbox(ctx, q, 10)
	if err != nil || n != 0 {
		t.Fatalf("early publish: %d %v", n, err)
	}
	if _, err = s.claim(ctx, j.ID, time.Second); err != pgx.ErrNoRows {
		t.Fatalf("early claim: %v", err)
	}
	w := &Worker{Store: s, Queue: q, Concurrency: 1, Lease: time.Second, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	go w.run(ctx)
	go (&Dispatcher{Store: s, Queue: q, Logger: w.Logger}).run(ctx)
	finished := waitStatus(t, s, j.ID, "failed")
	if finished.Attempts != 1 {
		t.Fatalf("attempts=%d", finished.Attempts)
	}
}

func TestOutboxSurvivesRedisFailure(t *testing.T) {
	s, q := testServices(t)
	ctx := context.Background()
	j, _, err := s.submit(ctx, request("sleep", `{"duration_ms":0}`))
	if err != nil {
		t.Fatal(err)
	}
	bad := &Queue{Client: redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0, DialTimeout: 100 * time.Millisecond})}
	defer bad.Client.Close()
	if _, err = s.publishOutbox(ctx, bad, 10); err == nil {
		t.Fatal("expected Redis failure")
	}
	var pending int
	if err = s.DB.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE delivered_at IS NULL`).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("pending=%d %v", pending, err)
	}
	if _, err = s.publishOutbox(ctx, q, 10); err != nil {
		t.Fatal(err)
	}
	id, err := q.pop(ctx)
	if err != nil || id != j.ID {
		t.Fatalf("recovered publish: %s %v", id, err)
	}
}

func TestPriorityCycle(t *testing.T) {
	_, q := testServices(t)
	ctx := context.Background()
	for _, p := range []string{"high", "normal", "low"} {
		for i := 0; i < 20; i++ {
			if err := q.publish(ctx, readyJob{ID: newUUID(), Priority: p, RunAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
		}
	}
	counts := map[string]int{}
	for i := 0; i < 7; i++ {
		id, err := q.pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{"high", "normal", "low"} {
			// Find which queue lost one item by observing cardinality.
			n, err := q.Client.ZCard(ctx, "ready:"+p).Result()
			if err != nil {
				t.Fatal(err)
			}
			if n == int64(19-counts[p]) {
				counts[p]++
				break
			}
		}
		if id == "" {
			t.Fatal("empty pop")
		}
	}
	if counts["high"] != 4 || counts["normal"] != 2 || counts["low"] != 1 {
		t.Fatalf("priority distribution: %#v", counts)
	}
}

func TestRequestValidation(t *testing.T) {
	for _, tc := range []struct {
		typ, payload string
		valid        bool
	}{
		{"sleep", `{"duration_ms":100}`, true},
		{"sleep", `{"duration_ms":300001}`, false},
		{"record_event", `{"name":""}`, false},
		{"unknown", `{}`, false},
	} {
		r := SubmitRequest{Type: tc.typ, Payload: json.RawMessage(tc.payload)}
		if (r.normalize() == nil) != tc.valid {
			t.Errorf("validation of %s %s", tc.typ, tc.payload)
		}
	}
}

func TestRESTSubmissionAndStatus(t *testing.T) {
	s, q := testServices(t)
	server := httptest.NewServer((&API{Store: s, Queue: q, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}).routes())
	defer server.Close()
	post := func(body string) (int, Job) {
		t.Helper()
		resp, err := http.Post(server.URL+"/jobs", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var j Job
		if resp.StatusCode < 300 {
			if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
				t.Fatal(err)
			}
		}
		return resp.StatusCode, j
	}
	body := `{"type":"sleep","payload":{"duration_ms":0},"idempotency_key":"api-test"}`
	code, j := post(body)
	if code != http.StatusAccepted || j.ID == "" {
		t.Fatalf("first: %d %+v", code, j)
	}
	code, again := post(body)
	if code != http.StatusOK || again.ID != j.ID {
		t.Fatalf("repeat: %d %+v", code, again)
	}
	code, _ = post(`{"type":"sleep","payload":{"duration_ms":1},"idempotency_key":"api-test"}`)
	if code != http.StatusConflict {
		t.Fatalf("conflict: %d", code)
	}
	code, _ = post(`{"type":"sleep","max_attempts":0}`)
	if code != http.StatusBadRequest {
		t.Fatalf("invalid attempts: %d", code)
	}
	resp, err := http.Get(server.URL + "/jobs/" + j.ID)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	resp, err = http.Get(server.URL + "/jobs/invalid-id")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid ID: %d", resp.StatusCode)
	}
}
