// Package resilient provides an http.RoundTripper that retries failed requests with jittered
// backoff, honours Retry-After, and trips a circuit breaker when a dependency is consistently down.
//
// It is a RoundTripper rather than a wrapper around http.Client on purpose. A RoundTripper composes:
// it drops into any existing client, stacks with other middleware (tracing, auth, metrics), and works
// unchanged with any library that accepts an *http.Client. Wrapping the client instead would mean
// every caller has to adopt a bespoke type.
//
//	client := &http.Client{
//	    Transport: resilient.New(
//	        resilient.WithMaxAttempts(4),
//	        resilient.WithBreaker(resilient.NewCircuitBreaker()),
//	    ),
//	}
//	resp, err := client.Get("https://example.com/api")
package resilient

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Transport is an http.RoundTripper that retries failed requests.
//
// The zero value is not usable; construct one with New.
type Transport struct {
	next        http.RoundTripper
	maxAttempts int
	backoff     Backoff
	retrier     Retrier
	breaker     *CircuitBreaker
	clock       Clock
	perAttempt  time.Duration
	respectRA   bool
	maxRA       time.Duration
	onRetry     func(Attempt)

	// rand is not safe for concurrent use, so it is guarded. A RoundTripper is called from many
	// goroutines at once, and a data race here would be both real and very hard to reproduce.
	mu  sync.Mutex
	rnd *rand.Rand
}

// Attempt describes a failed attempt that is about to be retried, for observability hooks.
type Attempt struct {
	Request  *http.Request
	Response *http.Response // nil if the transport failed outright
	Err      error          // nil if a response came back with a retryable status
	Number   int            // 1 for the first attempt
	Delay    time.Duration  // how long until the next attempt
}

// New builds a Transport with the given options.
//
// The defaults are deliberately cautious: three attempts, exponential backoff with full jitter,
// no retrying of non-idempotent methods, and no circuit breaker.
func New(opts ...Option) *Transport {
	t := &Transport{
		next:        http.DefaultTransport,
		maxAttempts: 3,
		backoff:     NewExponentialBackoff(),
		retrier:     DefaultRetrier{},
		clock:       SystemClock(),
		respectRA:   true,
		maxRA:       30 * time.Second,
		rnd:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.breaker != nil && !t.breaker.Allow() {
		return nil, ErrCircuitOpen
	}

	resp, err := t.attempt(req)

	if t.breaker != nil {
		// A retryable status counts as a failure for the breaker even though a response came back:
		// a dependency returning 503 to everything is down, whatever the transport thinks.
		t.breaker.Record(err == nil && !t.retrier.ShouldRetry(req, resp, nil))
	}
	return resp, err
}

func (t *Transport) attempt(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	for attempt := 1; ; attempt++ {
		// Each attempt needs its own body reader. See replayable for why this is the subtle part.
		perAttemptReq, cancel, err := t.prepare(req, attempt)
		if err != nil {
			return nil, err
		}

		resp, err := t.next.RoundTrip(perAttemptReq)

		// Distinguish our own per-attempt timeout from the caller's deadline before classifying.
		// Both arrive as context.DeadlineExceeded but mean opposite things; see ErrAttemptTimeout.
		err = classifyAttemptError(ctx, err)

		if !t.retrier.ShouldRetry(perAttemptReq, resp, err) || attempt >= t.maxAttempts {
			// Terminal, one way or the other. The per-attempt timeout must outlive the response,
			// since the caller still has to read the body, so cancellation is deferred to the body.
			if cancel != nil {
				bindCancelToBody(resp, cancel)
			}
			return resp, err
		}

		delay := t.delayFor(attempt, resp)

		if t.onRetry != nil {
			t.onRetry(Attempt{Request: perAttemptReq, Response: resp, Err: err, Number: attempt, Delay: delay})
		}

		// The response is being discarded, so its body must be drained and closed. Skipping this
		// leaks the connection: net/http only returns a connection to the pool once its body is read
		// to EOF and closed, so a retry loop that drops bodies quietly exhausts the pool and then
		// stalls — a failure that looks like the dependency being slow rather than a client bug.
		drain(resp)
		if cancel != nil {
			cancel()
		}

		if err := t.clock.Sleep(ctx, delay); err != nil {
			// The caller went away mid-backoff. Return their error, not a retry error.
			return nil, err
		}
	}
}

// prepare builds the request for one attempt, rewinding the body and applying any per-attempt
// timeout.
func (t *Transport) prepare(req *http.Request, attempt int) (*http.Request, func(), error) {
	out := req
	if attempt > 1 {
		rewound, err := replayable(req)
		if err != nil {
			return nil, nil, err
		}
		out = rewound
	}

	if t.perAttempt <= 0 {
		return out, nil, nil
	}

	// A per-attempt timeout is not the same as the caller's overall deadline. Without it, one
	// attempt that hangs consumes the entire budget and the remaining attempts never happen — the
	// retry configuration silently does nothing in exactly the case it was meant for.
	ctx, cancel := contextWithTimeout(out.Context(), t.perAttempt)
	return out.WithContext(ctx), cancel, nil
}

// delayFor decides how long to wait before the next attempt.
func (t *Transport) delayFor(attempt int, resp *http.Response) time.Duration {
	if t.respectRA {
		// A server that sends Retry-After knows more than any local calculation does — a rate
		// limiter knows exactly when the budget refills. Prefer it over the computed backoff.
		if d, ok := retryAfter(resp, t.clock.Now()); ok {
			return capDuration(d, t.maxRA)
		}
	}

	t.mu.Lock()
	delay := t.backoff.Delay(attempt, t.rnd)
	t.mu.Unlock()
	return delay
}

// replayable returns a copy of req with its body rewound to the start.
//
// This is the subtlety most retry wrappers get wrong. A request body is an io.ReadCloser, and the
// first attempt consumes it. Retrying with the same request sends an empty body, and the server sees
// a malformed request rather than the retry the client intended — silently, with no error anywhere.
//
// net/http solves this with GetBody, which it populates automatically for the common body types
// (bytes.Buffer, bytes.Reader, strings.Reader) precisely so redirects and HTTP/2 retries can replay
// them. Using it means the common cases work with no ceremony.
//
// A body that cannot be rewound — an arbitrary io.Reader, an open file, a streaming upload — has no
// GetBody, and this returns an error rather than silently sending nothing. Callers who need to retry
// such a request must buffer it themselves and accept the memory cost, which is a decision only they
// can make.
func replayable(req *http.Request) (*http.Request, error) {
	clone := req.Clone(req.Context())

	if req.Body == nil || req.Body == http.NoBody {
		return clone, nil
	}
	if req.GetBody == nil {
		return nil, fmt.Errorf(
			"resilient: cannot retry %s %s: request body is not replayable (no GetBody). "+
				"Buffer the body yourself, or use a body type net/http can rewind "+
				"(bytes.Reader, bytes.Buffer, strings.Reader)",
			req.Method, req.URL)
	}

	body, err := req.GetBody()
	if err != nil {
		return nil, fmt.Errorf("resilient: rewinding request body: %w", err)
	}
	clone.Body = body
	return clone, nil
}

// drain reads and closes a response body that is being discarded.
//
// The read is capped: the point is to let net/http return the connection to the pool, and a
// multi-megabyte error page is not worth the bandwidth to reuse one connection.
func drain(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
}

// capDuration clamps d into [0, max], guarding against the overflow that a very large
// Retry-After header can otherwise produce.
func capDuration(d, max time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	if max > 0 && d > max {
		return max
	}
	if d > math.MaxInt64/2 {
		return max
	}
	return d
}
