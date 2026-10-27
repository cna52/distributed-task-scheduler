package main

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func testObjects(t *testing.T) *ObjectStore {
	t.Helper()
	endpoint := os.Getenv("TEST_OBJECT_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TEST_OBJECT_ENDPOINT for image integration tests")
	}
	s, err := openObjectStore(context.Background(), endpoint, os.Getenv("TEST_OBJECT_ACCESS_KEY"), os.Getenv("TEST_OBJECT_SECRET_KEY"), os.Getenv("TEST_OBJECT_BUCKET"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func samplePNG() []byte {
	im := image.NewRGBA(image.Rect(0, 0, 40, 20))
	for y := 0; y < 20; y++ {
		for x := 0; x < 40; x++ {
			im.Set(x, y, color.RGBA{R: uint8(x * 5), G: uint8(y * 10), B: 80, A: 255})
		}
	}
	var b bytes.Buffer
	if err := png.Encode(&b, im); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func putTestObject(t *testing.T, o *ObjectStore, key string, data []byte) {
	t.Helper()
	_, err := o.Client.PutObject(context.Background(), o.Bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Client.RemoveObject(context.Background(), o.Bucket, key, minio.RemoveObjectOptions{}) })
}

func waitRunning(t *testing.T, s *Store, id string) Job { return waitStatus(t, s, id, "running") }

func TestGracefulWorkerDrain(t *testing.T) {
	s, q := testServices(t)
	ctx := context.Background()
	j, _, err := s.submit(ctx, request("sleep", `{"duration_ms":350}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.publishOutbox(ctx, q, 10); err != nil {
		t.Fatal(err)
	}
	intake, stop := context.WithCancel(ctx)
	w := &Worker{Store: s, Queue: q, Concurrency: 1, Lease: time.Second, ShutdownGrace: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	done := make(chan struct{})
	go func() { w.run(intake); close(done) }()
	waitRunning(t, s, j.ID)
	stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not drain")
	}
	finished := waitStatus(t, s, j.ID, "succeeded")
	if finished.Attempts != 1 {
		t.Fatalf("attempts=%d", finished.Attempts)
	}
}

func TestWorkerGraceDeadlineUsesLeaseRecovery(t *testing.T) {
	s, q := testServices(t)
	ctx := context.Background()
	j, _, err := s.submit(ctx, request("sleep", `{"duration_ms":500}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.publishOutbox(ctx, q, 10); err != nil {
		t.Fatal(err)
	}
	intake, stop := context.WithCancel(ctx)
	w := &Worker{Store: s, Queue: q, Concurrency: 1, Lease: 400 * time.Millisecond, ShutdownGrace: 50 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	done := make(chan struct{})
	go func() { w.run(intake); close(done) }()
	waitRunning(t, s, j.ID)
	stop()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("worker exceeded shutdown grace")
	}
	state, err := s.getJob(ctx, j.ID)
	if err != nil || state.Status != "running" {
		t.Fatalf("unexpected state after deadline: %+v %v", state, err)
	}
	time.Sleep(450 * time.Millisecond)
	if n, err := s.expireLeases(ctx, 10); err != nil || n != 1 {
		t.Fatalf("expire: %d %v", n, err)
	}
	if state, err = s.getJob(ctx, j.ID); err != nil || state.Status != "queued" {
		t.Fatalf("not queued after expiry: %+v %v", state, err)
	}
}

func TestBatchPublishAndQueueMetrics(t *testing.T) {
	s, q := testServices(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if _, _, err := s.submit(ctx, request("sleep", `{"duration_ms":0}`)); err != nil {
			t.Fatal(err)
		}
	}
	m, err := s.queueMetrics(ctx)
	if err != nil || m.Pending != 20 || m.OldestSeconds < 0 {
		t.Fatalf("prepublish metrics: %+v %v", m, err)
	}
	n, err := s.publishOutbox(ctx, q, 20)
	if err != nil || n != 20 {
		t.Fatalf("batch publish: %d %v", n, err)
	}
	m, err = s.queueMetrics(ctx)
	if err != nil || m.Pending != 0 {
		t.Fatalf("postpublish metrics: %+v %v", m, err)
	}
	count, err := q.Client.ZCard(ctx, "ready:normal").Result()
	if err != nil || count != 20 {
		t.Fatalf("redis ready=%d %v", count, err)
	}
	api := &API{Store: s, Queue: q, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	w := httptest.NewRecorder()
	api.routes().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, metric := range []string{"scheduler_outbox_pending 0", "scheduler_outbox_oldest_pending_seconds", "scheduler_submission_to_first_start_seconds_p95", "scheduler_submission_to_first_start_samples"} {
		if !strings.Contains(w.Body.String(), metric) {
			t.Fatalf("missing metric %s", metric)
		}
	}
}

func TestResizeImageReplayAndSharedStore(t *testing.T) {
	s, q := testServices(t)
	o1 := testObjects(t)
	o2 := testObjects(t)
	ctx := context.Background()
	source := "inputs/" + newUUID() + ".png"
	putTestObject(t, o1, source, samplePNG())
	payload, _ := json.Marshal(resizePayload{SourceKey: source, Width: 10, Height: 10})
	r := request("resize_image", string(payload))
	j1, _, err := s.submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	j2, _, err := s.submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.publishOutbox(ctx, q, 10); err != nil {
		t.Fatal(err)
	}
	// Separate worker instances and storage clients process distinct jobs.
	for i, pair := range []struct {
		j Job
		o *ObjectStore
	}{{j1, o1}, {j2, o2}} {
		claimed, err := s.claim(ctx, pair.j.ID, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		worker := &Worker{Store: s, Queue: q, Objects: pair.o, Lease: 2 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		worker.process(ctx, claimed)
		finished, err := s.getJob(ctx, pair.j.ID)
		if err != nil || finished.Status != "succeeded" {
			t.Fatalf("worker %d state: %+v %v", i, finished, err)
		}
		result := finished.Result
		var out struct {
			OutputKey string `json:"output_key"`
			Width     int    `json:"width"`
			Height    int    `json:"height"`
		}
		if err := json.Unmarshal(result, &out); err != nil {
			t.Fatal(err)
		}
		if out.Width != 10 || out.Height != 5 || out.OutputKey != "jobs/"+pair.j.ID+"/thumbnail.png" {
			t.Fatalf("result: %+v", out)
		}
		obj, err := o2.Client.GetObject(ctx, o2.Bucket, out.OutputKey, minio.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		config, err := png.DecodeConfig(obj)
		obj.Close()
		if err != nil || config.Width != 10 || config.Height != 5 {
			t.Fatalf("output: %+v %v", config, err)
		}
		t.Cleanup(func() {
			o1.Client.RemoveObject(context.Background(), o1.Bucket, out.OutputKey, minio.RemoveObjectOptions{})
		})
	}
	// Crash after the object write but before the PostgreSQL completion commit.
	j3, _, err := s.submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.claim(ctx, j3.ID, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	firstResult, err := execute(ctx, first, o1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(140 * time.Millisecond)
	if n, err := s.expireLeases(ctx, 10); err != nil || n != 1 {
		t.Fatalf("expire: %d %v", n, err)
	}
	time.Sleep(1050 * time.Millisecond)
	second, err := s.claim(ctx, j3.ID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	secondResult, err := execute(ctx, second, o2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstResult, secondResult) {
		t.Fatalf("replay result changed: %s vs %s", firstResult, secondResult)
	}
	if ok, err := s.complete(ctx, second, secondResult); err != nil || !ok {
		t.Fatalf("replay complete: %v %v", ok, err)
	}
	if ok, err := s.complete(ctx, first, firstResult); err != nil || ok {
		t.Fatalf("stale complete: %v %v", ok, err)
	}
	objects := o1.Client.ListObjects(ctx, o1.Bucket, minio.ListObjectsOptions{Prefix: "jobs/" + j3.ID + "/", Recursive: true})
	count := 0
	for obj := range objects {
		if obj.Err != nil {
			t.Fatal(obj.Err)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("replay created %d output objects", count)
	}
	t.Cleanup(func() {
		o1.Client.RemoveObject(context.Background(), o1.Bucket, "jobs/"+j3.ID+"/thumbnail.png", minio.RemoveObjectOptions{})
	})
}

func TestResizeImageInvalidInput(t *testing.T) {
	for _, p := range []resizePayload{{SourceKey: "../secret", Width: 10, Height: 10}, {SourceKey: "inputs/a", Width: 0, Height: 10}, {SourceKey: "inputs/../a", Width: 10, Height: 10}} {
		if p.validate() == nil {
			t.Fatalf("accepted %+v", p)
		}
	}
	o := testObjects(t)
	ctx := context.Background()
	wide := image.NewRGBA(image.Rect(0, 0, 8001, 1))
	var widePNG bytes.Buffer
	if err := png.Encode(&widePNG, wide); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		data    []byte
		missing bool
	}{{data: []byte("not an image")}, {data: make([]byte, maxInputBytes+1)}, {data: widePNG.Bytes()}} {
		key := "inputs/" + newUUID() + ".png"
		if !tc.missing {
			putTestObject(t, o, key, tc.data)
		}
		p, _ := json.Marshal(resizePayload{SourceKey: key, Width: 10, Height: 10})
		j := Job{ID: newUUID(), Payload: p}
		if _, err := o.resize(ctx, j); err == nil {
			t.Fatal("invalid image accepted")
		}
	}
	missing, _ := json.Marshal(resizePayload{SourceKey: "inputs/missing-" + newUUID() + ".png", Width: 10, Height: 10})
	if _, err := o.resize(ctx, Job{ID: newUUID(), Payload: missing}); err == nil {
		t.Fatal("missing input accepted")
	}
}

func TestResizeImageMissingInputExhaustsAttempts(t *testing.T) {
	s, q := testServices(t)
	o := testObjects(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, _ := json.Marshal(resizePayload{SourceKey: "inputs/missing-" + newUUID() + ".png", Width: 10, Height: 10})
	r := request("resize_image", string(p))
	one := 1
	r.MaxAttempts = &one
	j, _, err := s.submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.publishOutbox(ctx, q, 10); err != nil {
		t.Fatal(err)
	}
	w := &Worker{Store: s, Queue: q, Objects: o, Concurrency: 1, Lease: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go w.run(ctx)
	finished := waitStatus(t, s, j.ID, "failed")
	if finished.Attempts != 1 || finished.Error == nil {
		t.Fatalf("failed state: %+v", finished)
	}
}
