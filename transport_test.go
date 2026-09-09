package resilient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestTransport builds a Transport with a fake clock and deterministic jitter, so tests assert on
// exact delays and never actually wait.
func newTestTransport(t *testing.T, clock *fakeClock, opts ...Option) *Transport {
	t.Helper()
	base := []Option{
		WithClock(clock),
		WithRand(rand.New(rand.NewSource(1))),
		WithBackoff(&ExponentialBackoff{Base: 100 * time.Millisecond, Max: time.Minute, Jitter: JitterNone}),
	}
	return New(append(base, opts...)...)
}

func TestSucceedsWithoutRetrying(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock)}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if calls != 1 {
		t.Errorf("server saw %d requests, want 1", calls)
	}
	if len(clock.sleeps()) != 0 {
		t.Errorf("slept %v on a successful request", clock.sleeps())
	}
}

func TestRetriesUntilSuccess(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "recovered")
	}))
	defer srv.Close()

	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock, WithMaxAttempts(5))}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "recovered" {
		t.Errorf("body = %q, want %q", body, "recovered")
	}
	if calls != 3 {
		t.Errorf("server saw %d requests, want 3", calls)
	}

	// Asserting the exact schedule is only possible because the clock records rather than waits.
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
	got := clock.sleeps()
	if len(got) != len(want) {
		t.Fatalf("sleeps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sleep %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock, WithMaxAttempts(3))}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if calls != 3 {
		t.Errorf("server saw %d requests, want 3", calls)
	}
	// The last response is returned rather than swallowed: the caller may well want to inspect it.
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestDoesNotRetryNonRetryableStatus(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	client := &http.Client{Transport: newTestTransport(t, newFakeClock(), WithMaxAttempts(5))}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if calls != 1 {
		t.Errorf("server saw %d requests; a 400 will be a 400 next time too", calls)
	}
}

// The subtlety most retry wrappers get wrong: the first attempt consumes the body, so a naive retry
// sends an empty one and the server sees a malformed request with no error anywhere.
func TestReplaysRequestBodyOnRetry(t *testing.T) {
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		if len(bodies) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock,
		WithMaxAttempts(5),
		WithRetrier(DefaultRetrier{RetryNonIdempotent: true}))}

	// strings.Reader is one of the types net/http populates GetBody for.
	resp, err := client.Post(srv.URL, "text/plain", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if len(bodies) != 3 {
		t.Fatalf("server saw %d requests, want 3", len(bodies))
	}
	for i, b := range bodies {
		if b != "payload" {
			t.Errorf("attempt %d body = %q, want %q (the body was not rewound)", i+1, b, "payload")
		}
	}
}

// A body with no GetBody cannot be rewound. Failing loudly beats silently sending nothing.
func TestRefusesToRetryUnreplayableBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	// An opaque io.Reader: net/http cannot populate GetBody for this.
	req, err := http.NewRequest(http.MethodPost, srv.URL, io.LimitReader(rand.New(rand.NewSource(1)), 16))
	if err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Transport: newTestTransport(t, newFakeClock(),
		WithMaxAttempts(3),
		WithRetrier(DefaultRetrier{RetryNonIdempotent: true}))}

	_, err = client.Do(req)
	if err == nil {
		t.Fatal("expected an error rather than a silently empty retry")
	}
	if !strings.Contains(err.Error(), "not replayable") {
		t.Errorf("error = %v, want it to explain the body is not replayable", err)
	}
}

func TestBodylessRequestsRetryFine(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 2 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := &http.Client{Transport: newTestTransport(t, newFakeClock(), WithMaxAttempts(3))}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if calls != 2 {
		t.Errorf("server saw %d requests, want 2", calls)
	}
}

// A server that says when to come back knows more than any local backoff calculation.
func TestHonoursRetryAfter(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 2 {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock, WithMaxAttempts(3))}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	got := clock.sleeps()
	if len(got) != 1 || got[0] != 7*time.Second {
		t.Errorf("sleeps = %v, want [7s] from the Retry-After header", got)
	}
}

// An unbounded Retry-After lets a server park the client for hours.
func TestCapsRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "86400") // a full day
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock,
		WithMaxAttempts(2),
		WithRetryAfter(true, 5*time.Second))}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	got := clock.sleeps()
	if len(got) != 1 || got[0] != 5*time.Second {
		t.Errorf("sleeps = %v, want [5s] (the cap), not a day", got)
	}
}

func TestRetryAfterCanBeDisabled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock,
		WithMaxAttempts(2),
		WithRetryAfter(false, 0))}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	got := clock.sleeps()
	if len(got) != 1 || got[0] != 100*time.Millisecond {
		t.Errorf("sleeps = %v, want the computed [100ms] backoff", got)
	}
}

func TestStopsRetryingWhenContextCancelled(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock, WithMaxAttempts(10))}

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	cancel() // The caller gives up before the first backoff completes.

	_, err := client.Do(req)
	if err == nil {
		t.Fatal("expected an error after cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want it to wrap context.Canceled", err)
	}
	if calls > 2 {
		t.Errorf("server saw %d requests; retrying past a cancellation holds up the caller", calls)
	}
}

func TestOnRetryHookSeesEveryRetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	var attempts []Attempt
	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock,
		WithMaxAttempts(3),
		WithOnRetry(func(a Attempt) { attempts = append(attempts, a) }))}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	// Three attempts means two retries, so two hook calls.
	if len(attempts) != 2 {
		t.Fatalf("hook fired %d times, want 2", len(attempts))
	}
	if attempts[0].Number != 1 || attempts[1].Number != 2 {
		t.Errorf("attempt numbers = %d, %d; want 1, 2", attempts[0].Number, attempts[1].Number)
	}
	if attempts[0].Delay != 100*time.Millisecond {
		t.Errorf("first delay = %v, want 100ms", attempts[0].Delay)
	}
}

// net/http only returns a connection to the pool once its body is read to EOF and closed. A retry
// loop that drops response bodies exhausts the pool and then stalls — which looks like the
// dependency being slow rather than a client bug.
func TestDrainsDiscardedResponseBodies(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 5 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, strings.Repeat("error detail ", 100))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// One connection allowed. If discarded bodies were not drained and closed, the second attempt
	// would have no connection available and the test would hang until its deadline.
	base := &http.Transport{MaxIdleConns: 1, MaxConnsPerHost: 1}
	defer base.CloseIdleConnections()

	clock := newFakeClock()
	client := &http.Client{
		Transport: newTestTransport(t, clock, WithMaxAttempts(6), WithTransport(base)),
		Timeout:   10 * time.Second,
	}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error (a leaked connection would show up as a timeout here): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// The returned response's body must still be readable; cancelling the per-attempt context on the way
// out would kill it and surface as "context canceled" on the caller's first Read.
func TestReturnedBodyRemainsReadableWithPerAttemptTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "the payload")
	}))
	defer srv.Close()

	client := &http.Client{Transport: newTestTransport(t, newFakeClock(),
		WithPerAttemptTimeout(5*time.Second))}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the body failed: %v", err)
	}
	if string(body) != "the payload" {
		t.Errorf("body = %q, want %q", body, "the payload")
	}
}

func TestPerAttemptTimeoutBoundsAHangingAttempt(t *testing.T) {
	release := make(chan struct{})
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			// The first attempt hangs until the test lets it go.
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	// A real clock here: the point is that the per-attempt timeout genuinely fires.
	client := &http.Client{Transport: New(
		WithMaxAttempts(2),
		WithPerAttemptTimeout(150*time.Millisecond),
		WithBackoff(NoBackoff{}),
	)}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 from the second attempt", resp.StatusCode)
	}
	if calls != 2 {
		t.Errorf("server saw %d requests, want 2", calls)
	}
}

func TestPostIsNotRetriedByDefault(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client := &http.Client{Transport: newTestTransport(t, newFakeClock(), WithMaxAttempts(5))}
	resp, err := client.Post(srv.URL, "text/plain", bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if calls != 1 {
		t.Errorf("server saw %d POSTs; retrying one that may have been processed can double-charge", calls)
	}
}

// A RoundTripper is called from many goroutines at once; the shared *rand.Rand is the obvious hazard.
func TestTransportIsConcurrencySafe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	clock := newFakeClock()
	client := &http.Client{Transport: newTestTransport(t, clock, WithMaxAttempts(3))}

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(srv.URL)
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait() // The race detector is the assertion.
}

// End-to-end: once open, the transport must stop reaching the server entirely.
func TestTransportStopsCallingOpenCircuit(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	clock := newFakeClock()
	breaker := &CircuitBreaker{FailureThreshold: 2, OpenDuration: time.Minute, HalfOpenProbes: 1, Clock: clock}
	client := &http.Client{Transport: newTestTransport(t, clock,
		WithMaxAttempts(1),
		WithBreaker(breaker))}

	for i := 0; i < 2; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		resp.Body.Close()
	}

	before := atomic.LoadInt32(&calls)

	_, err := client.Get(srv.URL)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("error = %v, want ErrCircuitOpen", err)
	}
	if atomic.LoadInt32(&calls) != before {
		t.Error("an open breaker must not add load to a struggling dependency")
	}
}

// A dependency answering 503 to everything is down, whatever the transport layer thinks.
func TestRetryableStatusCountsAsBreakerFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	clock := newFakeClock()
	breaker := &CircuitBreaker{FailureThreshold: 2, OpenDuration: time.Minute, HalfOpenProbes: 1, Clock: clock}
	client := &http.Client{Transport: newTestTransport(t, clock, WithMaxAttempts(1), WithBreaker(breaker))}

	for i := 0; i < 2; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	if breaker.State() != BreakerOpen {
		t.Errorf("state = %v; repeated 503s should open the breaker", breaker.State())
	}
}

// sleeps lives here rather than beside the rest of fakeClock because these are its only
// callers: asserting on the exact backoff schedule is what the transport tests do, and the
// breaker tests only need the clock to advance.
// sleeps returns every duration Sleep was called with, in order.
func (c *fakeClock) sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.slept))
	copy(out, c.slept)
	return out
}
