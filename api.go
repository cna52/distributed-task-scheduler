package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type API struct {
	Store     *Store
	Queue     *Queue
	Logger    *slog.Logger
	submitted atomic.Uint64
	rejected  atomic.Uint64
}

func (a *API) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", a.submit)
	mux.HandleFunc("GET /jobs/{id}", a.status)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := a.Store.check(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		io.WriteString(w, "# TYPE scheduler_submissions_total counter\n")
		io.WriteString(w, "scheduler_submissions_total "+strconv.FormatUint(a.submitted.Load(), 10)+"\n")
		io.WriteString(w, "# TYPE scheduler_rejections_total counter\n")
		io.WriteString(w, "scheduler_rejections_total "+strconv.FormatUint(a.rejected.Load(), 10)+"\n")
		if m, err := a.Store.queueMetrics(r.Context()); err == nil {
			io.WriteString(w, "# TYPE scheduler_outbox_pending gauge\n")
			io.WriteString(w, "scheduler_outbox_pending "+strconv.FormatInt(m.Pending, 10)+"\n")
			io.WriteString(w, "# TYPE scheduler_outbox_oldest_pending_seconds gauge\n")
			io.WriteString(w, "scheduler_outbox_oldest_pending_seconds "+strconv.FormatFloat(m.OldestSeconds, 'f', 3, 64)+"\n")
			io.WriteString(w, "# TYPE scheduler_submission_to_first_start_seconds_p95 gauge\n")
			io.WriteString(w, "scheduler_submission_to_first_start_seconds_p95 "+strconv.FormatFloat(m.FirstStartP95Seconds, 'f', 3, 64)+"\n")
			io.WriteString(w, "# TYPE scheduler_submission_to_first_start_samples gauge\n")
			io.WriteString(w, "scheduler_submission_to_first_start_samples "+strconv.FormatInt(m.StartedSamples, 10)+"\n")
		}
		rows, err := a.Store.DB.Query(r.Context(), `SELECT status,count(*) FROM jobs GROUP BY status`)
		if err == nil {
			defer rows.Close()
			io.WriteString(w, "# TYPE scheduler_jobs gauge\n")
			for rows.Next() {
				var status string
				var count int64
				if rows.Scan(&status, &count) == nil {
					io.WriteString(w, "scheduler_jobs{status=\""+status+"\"} "+strconv.FormatInt(count, 10)+"\n")
				}
			}
		}
		if n, err := a.Queue.Client.ZCard(r.Context(), "ready:high").Result(); err == nil {
			io.WriteString(w, "# TYPE scheduler_redis_ready gauge\n")
			io.WriteString(w, "scheduler_redis_ready{priority=\"high\"} "+strconv.FormatInt(n, 10)+"\n")
			for _, priority := range []string{"normal", "low"} {
				if n, err := a.Queue.Client.ZCard(r.Context(), "ready:"+priority).Result(); err == nil {
					io.WriteString(w, "scheduler_redis_ready{priority=\""+priority+"\"} "+strconv.FormatInt(n, 10)+"\n")
				}
			}
		}
	})
	return mux
}

func (a *API) submit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req SubmitRequest
	if err := dec.Decode(&req); err != nil {
		a.bad(w, err.Error())
		return
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		a.bad(w, "request must contain one JSON object")
		return
	}
	if err := req.normalize(); err != nil {
		a.bad(w, err.Error())
		return
	}
	if req.RunAt != nil && req.RunAt.After(time.Now().Add(365*24*time.Hour)) {
		a.bad(w, "run_at must be within one year")
		return
	}
	j, existing, err := a.Store.submit(r.Context(), req)
	if errors.Is(err, errConflict) {
		a.rejected.Add(1)
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		a.Logger.Error("submit", "error", err)
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	a.submitted.Add(1)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", "/jobs/"+j.ID)
	if existing {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusAccepted)
	}
	json.NewEncoder(w).Encode(j)
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validUUID(id) {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}
	j, err := a.Store.getJob(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	if err != nil {
		a.Logger.Error("status", "error", err)
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}

func validUUID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	return err == nil
}

func (a *API) bad(w http.ResponseWriter, msg string) {
	a.rejected.Add(1)
	http.Error(w, msg, http.StatusBadRequest)
}
