package resilient

import (
	"context"
	"math/rand"
	"net/http"
	"time"
)

// Option configures a Transport.
//
// Functional options rather than an exported config struct: it keeps the zero value from being a
// half-configured Transport, lets defaults live in one place, and means adding an option later is not
// a breaking change to anyone's struct literal.
type Option func(*Transport)

// WithTransport sets the RoundTripper that actually performs requests.
//
// Defaults to http.DefaultTransport. Use this to stack middleware — tracing or metrics beneath the
// retry layer will see every individual attempt, which is usually what you want.
func WithTransport(next http.RoundTripper) Option {
	return func(t *Transport) {
		if next != nil {
			t.next = next
		}
	}
}

// WithMaxAttempts sets the total number of attempts, including the first.
//
// So 1 disables retrying entirely, and 3 (the default) means the original request plus two retries.
// Counting the first attempt avoids the perennial off-by-one where "retries: 3" means three or four
// requests depending on who wrote the library.
func WithMaxAttempts(n int) Option {
	return func(t *Transport) {
		if n >= 1 {
			t.maxAttempts = n
		}
	}
}

// WithBackoff sets how long to wait between attempts.
func WithBackoff(b Backoff) Option {
	return func(t *Transport) {
		if b != nil {
			t.backoff = b
		}
	}
}

// WithRetrier sets the policy deciding which failures are retried.
func WithRetrier(r Retrier) Option {
	return func(t *Transport) {
		if r != nil {
			t.retrier = r
		}
	}
}

// WithBreaker attaches a circuit breaker.
//
// Off by default, because a breaker changes failure behaviour in a way that should be a deliberate
// choice: once open it returns ErrCircuitOpen without attempting the request at all.
func WithBreaker(b *CircuitBreaker) Option {
	return func(t *Transport) { t.breaker = b }
}

// WithPerAttemptTimeout bounds how long any single attempt may take.
//
// Distinct from the caller's overall deadline, and worth setting. Without it a single hung attempt
// consumes the whole context budget and the remaining attempts never run — so the retry configuration
// silently does nothing in precisely the situation it exists for.
func WithPerAttemptTimeout(d time.Duration) Option {
	return func(t *Transport) {
		if d > 0 {
			t.perAttempt = d
		}
	}
}

// WithRetryAfter controls whether a server's Retry-After header overrides the computed backoff, and
// caps how long it will be honoured for.
//
// Enabled by default with a 30s cap. The cap matters: an unbounded Retry-After lets a server — or
// anything able to inject a header — park the client for hours.
func WithRetryAfter(respect bool, max time.Duration) Option {
	return func(t *Transport) {
		t.respectRA = respect
		if max > 0 {
			t.maxRA = max
		}
	}
}

// WithClock replaces the time source. Intended for tests.
func WithClock(c Clock) Option {
	return func(t *Transport) {
		if c != nil {
			t.clock = c
		}
	}
}

// WithRand sets the randomness used for backoff jitter. Seed it to make retry timing reproducible in
// tests.
func WithRand(r *rand.Rand) Option {
	return func(t *Transport) {
		if r != nil {
			t.rnd = r
		}
	}
}

// WithOnRetry registers a callback invoked before each retry.
//
// Intended for metrics and logging. It runs on the calling goroutine, so a slow callback slows the
// request; keep it to counter increments and log lines.
func WithOnRetry(fn func(Attempt)) Option {
	return func(t *Transport) { t.onRetry = fn }
}

// contextWithTimeout exists so transport.go does not need the context import, and to keep the
// per-attempt timeout construction in one place.
func contextWithTimeout(parent context.Context, d time.Duration) (context.Context, func()) {
	return context.WithTimeout(parent, d)
}

// bindCancelToBody defers a context cancellation until the response body is closed.
//
// A per-attempt timeout is created with context.WithTimeout, and cancelling that context closes the
// response body. For the attempt that is ultimately returned to the caller, cancelling on the way out
// would hand back a response whose body is already dead — the caller would get "context canceled" on
// their first Read, which looks like a network failure rather than a client bug.
//
// So the cancel is attached to the body instead: it runs when the caller closes the response, which
// is exactly when the attempt is genuinely finished. If there is no body to hang it on, cancelling
// immediately is safe and prevents a context leak.
func bindCancelToBody(resp *http.Response, cancel func()) {
	if resp == nil || resp.Body == nil {
		cancel()
		return
	}
	resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
}
