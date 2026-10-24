package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Job struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Priority       string          `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	Attempts       int             `json:"attempts"`
	RunAt          time.Time       `json:"run_at"`
	LeasedUntil    *time.Time      `json:"leased_until,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	Error          *string         `json:"error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at"`
	IdempotencyKey *string         `json:"-"`
	RequestHash    string          `json:"-"`
	LeaseToken     *string         `json:"-"`
}

type SubmitRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       string          `json:"priority,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	MaxAttempts    *int            `json:"max_attempts,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

func (r *SubmitRequest) normalize() error {
	if r.Priority == "" {
		r.Priority = "normal"
	}
	if r.MaxAttempts == nil {
		v := 3
		r.MaxAttempts = &v
	}
	if r.Priority != "high" && r.Priority != "normal" && r.Priority != "low" {
		return errors.New("priority must be high, normal, or low")
	}
	if *r.MaxAttempts < 1 || *r.MaxAttempts > 20 {
		return errors.New("max_attempts must be between 1 and 20")
	}
	if len(r.IdempotencyKey) > 128 {
		return errors.New("idempotency_key exceeds 128 characters")
	}
	if len(r.Payload) == 0 {
		r.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(r.Payload) {
		return errors.New("payload must be valid JSON")
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(r.Payload, &data); err != nil || data == nil {
		return errors.New("payload must be a JSON object")
	}
	switch r.Type {
	case "sleep":
		var p struct {
			DurationMS int `json:"duration_ms"`
		}
		if err := json.Unmarshal(r.Payload, &p); err != nil || p.DurationMS < 0 || p.DurationMS > 300000 {
			return errors.New("sleep duration_ms must be 0..300000")
		}
	case "fail":
		var p struct {
			Failures int `json:"failures"`
		}
		if err := json.Unmarshal(r.Payload, &p); err != nil || p.Failures < 0 || p.Failures > 20 {
			return errors.New("fail failures must be 0..20")
		}
	case "record_event":
		var p struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(r.Payload, &p); err != nil || strings.TrimSpace(p.Name) == "" || len(p.Name) > 200 {
			return errors.New("record_event name must be 1..200 characters")
		}
	default:
		return fmt.Errorf("unknown job type %q", r.Type)
	}
	return nil
}

func (r SubmitRequest) hash() string {
	// Encoding a map through encoding/json gives a stable key order.
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Second * time.Duration(1<<(attempt-1))
}
