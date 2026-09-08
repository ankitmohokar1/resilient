package resilient

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when the breaker is open and the request was not attempted.
//
// It is a distinct error so callers can tell "your dependency is down and we are not even trying"
// apart from "we tried and it failed" — which are different operational situations and often want
// different handling.
var ErrCircuitOpen = errors.New("resilient: circuit breaker is open")

// BreakerState is the breaker's current position.
type BreakerState int

const (
	// BreakerClosed passes everything through. The normal state.
	BreakerClosed BreakerState = iota

	// BreakerOpen rejects everything immediately, without attempting the request.
	BreakerOpen

	// BreakerHalfOpen allows a limited number of probes through to test whether the dependency has
	// recovered.
	BreakerHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker stops sending requests to a dependency that is consistently failing.
//
// # What it is actually for
//
// Not primarily to fail the caller faster, though it does that. The point is to stop adding load to
// something that is already struggling, and to stop paying a connect timeout on every request while
// it is unreachable. A dependency at capacity recovers much faster if the traffic stops.
//
// # Why consecutive failures
//
// Only consecutive failures count toward opening. A single timeout under load is noise, and a breaker
// that trips on it flaps between open and closed — which is worse than having no breaker, because the
// behaviour becomes unpredictable exactly when someone is trying to reason about an incident.
//
// # Half-open
//
// After the open window elapses the breaker admits a small number of probes rather than reopening the
// floodgates. If a probe succeeds the breaker closes; if one fails it opens again for another full
// window. Limiting probes matters: slamming a just-recovered dependency with the full backlog is how
// a recovery becomes a second outage.
//
// A CircuitBreaker is safe for concurrent use.
type CircuitBreaker struct {
	// FailureThreshold is how many consecutive failures open the breaker. Defaults to 5.
	FailureThreshold int

	// OpenDuration is how long the breaker stays open before probing. Defaults to 10s.
	OpenDuration time.Duration

	// HalfOpenProbes is how many requests are admitted while half-open. Defaults to 1.
	HalfOpenProbes int

	// Clock is the time source. Defaults to SystemClock.
	Clock Clock

	mu             sync.Mutex
	state          BreakerState
	consecutive    int
	openedAt       time.Time
	probesInFlight int
}

// NewCircuitBreaker returns a breaker with the documented defaults.
func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{
		FailureThreshold: 5,
		OpenDuration:     10 * time.Second,
		HalfOpenProbes:   1,
		Clock:            SystemClock(),
	}
}

func (b *CircuitBreaker) clock() Clock {
	if b.Clock == nil {
		return SystemClock()
	}
	return b.Clock
}

func (b *CircuitBreaker) threshold() int {
	if b.FailureThreshold < 1 {
		return 5
	}
	return b.FailureThreshold
}

func (b *CircuitBreaker) openWindow() time.Duration {
	if b.OpenDuration <= 0 {
		return 10 * time.Second
	}
	return b.OpenDuration
}

func (b *CircuitBreaker) probeBudget() int {
	if b.HalfOpenProbes < 1 {
		return 1
	}
	return b.HalfOpenProbes
}

// Allow reports whether a request may proceed, and reserves a probe slot if half-open.
//
// Every Allow that returns true must be paired with exactly one Record, or the breaker leaks probe
// slots and never leaves half-open.
func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case BreakerClosed:
		return true

	case BreakerOpen:
		if b.clock().Now().Sub(b.openedAt) < b.openWindow() {
			return false
		}
		// The window has elapsed; move to half-open and let this caller be the first probe.
		b.state = BreakerHalfOpen
		b.probesInFlight = 1
		return true

	case BreakerHalfOpen:
		if b.probesInFlight >= b.probeBudget() {
			return false
		}
		b.probesInFlight++
		return true

	default:
		return true
	}
}

// Record reports the outcome of a request that Allow admitted.
func (b *CircuitBreaker) Record(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == BreakerHalfOpen && b.probesInFlight > 0 {
		b.probesInFlight--
	}

	if success {
		b.consecutive = 0
		// A successful probe closes the breaker: the dependency is answering again.
		b.state = BreakerClosed
		return
	}

	b.consecutive++

	if b.state == BreakerHalfOpen {
		// The probe failed, so the dependency has not recovered. Back to open for a full window
		// rather than continuing to probe, which would be a slow trickle of doomed requests.
		b.trip()
		return
	}
	if b.consecutive >= b.threshold() {
		b.trip()
	}
}

// trip opens the breaker. The caller must hold b.mu.
func (b *CircuitBreaker) trip() {
	b.state = BreakerOpen
	b.openedAt = b.clock().Now()
	b.probesInFlight = 0
}

// State returns the breaker's current state, for metrics and health endpoints.
func (b *CircuitBreaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Report half-open once the window has elapsed, so an observer is not told "open" about a breaker
	// that would in fact admit the next request.
	if b.state == BreakerOpen && b.clock().Now().Sub(b.openedAt) >= b.openWindow() {
		return BreakerHalfOpen
	}
	return b.state
}

// Reset returns the breaker to closed and forgets accumulated failures.
func (b *CircuitBreaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = BreakerClosed
	b.consecutive = 0
	b.probesInFlight = 0
}
