package main

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct{ Client *redis.Client }

func openQueue(ctx context.Context, url string) (*Queue, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	c := redis.NewClient(opt)
	return &Queue{Client: c}, nil
}

func (q *Queue) publish(ctx context.Context, j readyJob) error {
	if j.Priority != "high" && j.Priority != "normal" && j.Priority != "low" {
		return errors.New("invalid priority")
	}
	return q.Client.ZAddNX(ctx, "ready:"+j.Priority, redis.Z{Score: float64(j.RunAt.UnixMicro()), Member: j.ID}).Err()
}

// One Redis script owns selection and removal. The shared cycle guarantees a
// low-priority slot every seven successful pops when all queues are nonempty.
var popScript = redis.NewScript(`
local slots = {1,1,1,1,2,2,3}
local keys = {KEYS[1],KEYS[2],KEYS[3]}
local n = redis.call('INCR', KEYS[4])
local first = slots[((n-1) % 7)+1]
local order = {first,1,2,3}
for i=1,4 do
  local item = redis.call('ZPOPMIN', keys[order[i]], 1)
  if #item > 0 then return item[1] end
end
return nil
`)

func (q *Queue) pop(ctx context.Context) (string, error) {
	v, err := popScript.Run(ctx, q.Client, []string{"ready:high", "ready:normal", "ready:low", "ready:cycle"}).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if s, ok := v.(string); ok {
		return s, nil
	}
	return "", errors.New("unexpected Redis pop result")
}

func (q *Queue) check(ctx context.Context) error { return q.Client.Ping(ctx).Err() }

func idleWait(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
