package resilient

import (
	"context"
	"sync"
	"time"
)

// fakeClock records what it was asked to wait for instead of waiting.
//
// That recording is the point. It turns "did the backoff behave correctly" from a timing observation
// — inherently flaky, and slow in proportion to how correct the backoff is — into an assertion on an
// exact list of durations. The whole suite runs in milliseconds while exercising thirty-second caps.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	slept  []time.Duration
	blocks bool // when true, Sleep waits for a release instead of returning immediately
	relCh  chan struct{}
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.slept = append(c.slept, d)
	if d > 0 {
		c.now = c.now.Add(d)
	}
	blocks, ch := c.blocks, c.relCh
	c.mu.Unlock()

	// Even a fake clock must honour cancellation: a caller that has given up should not have a
	// backoff completed on its behalf, and tests rely on that to exercise the abandonment path.
	if !blocks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sleeps returns every duration Sleep was called with, in order.
func (c *fakeClock) sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.slept))
	copy(out, c.slept)
	return out
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
