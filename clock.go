package resilient

import (
	"context"
	"time"
)

// Clock is this package's view of time and of waiting.
//
// Both halves are here deliberately. A retrying client does not only read the clock, it sleeps —
// between attempts, and while a circuit breaker's open window elapses. Injecting "what time is it"
// but leaving time.Sleep scattered through the code leaves the test suite waiting in real time for
// backoffs that exist precisely to be long.
//
// With both behind one interface, tests drive five attempts of a thirty-second-capped exponential
// backoff instantly and assert on the exact delays requested, rather than on how long the test took.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// Sleep blocks for d, returning early with ctx.Err() if ctx is cancelled first.
	//
	// It takes a context because a caller who has given up should not be held for a backoff they no
	// longer care about, and because returning ctx.Err() lets the caller tell "the wait finished"
	// apart from "the caller went away".
	Sleep(ctx context.Context, d time.Duration) error
}

// systemClock is the real implementation, backed by the wall clock and a timer.
type systemClock struct{}

// SystemClock returns a Clock backed by the real clock.
func SystemClock() Clock { return systemClock{} }

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		// Still honour an already-cancelled context: a zero backoff must not smuggle through a
		// request the caller has abandoned.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	// A timer rather than time.Sleep, so cancellation is observed immediately instead of after the
	// full duration. Stopped explicitly because an un-stopped timer stays live in the runtime heap
	// until it fires, which for a thirty-second backoff on an abandoned request is a real leak.
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
