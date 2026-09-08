package resilient

import (
	"sync"
	"testing"
	"time"
)

func newTestBreaker(clock Clock) *CircuitBreaker {
	return &CircuitBreaker{FailureThreshold: 3, OpenDuration: 10 * time.Second, HalfOpenProbes: 1, Clock: clock}
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b := newTestBreaker(newFakeClock())

	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatalf("breaker opened after %d failures, want 3", i)
		}
		b.Record(false)
	}
	if b.State() != BreakerClosed {
		t.Errorf("state = %v after 2 failures, want closed", b.State())
	}

	b.Allow()
	b.Record(false)

	if b.State() != BreakerOpen {
		t.Errorf("state = %v after 3 failures, want open", b.State())
	}
	if b.Allow() {
		t.Error("an open breaker must not admit requests")
	}
}

// A breaker that trips on isolated failures flaps between states, which is worse than no breaker:
// the behaviour becomes unpredictable exactly when someone is debugging an incident.
func TestBreakerIgnoresNonConsecutiveFailures(t *testing.T) {
	b := newTestBreaker(newFakeClock())

	for i := 0; i < 20; i++ {
		b.Allow()
		b.Record(i%2 == 0) // fail, succeed, fail, succeed...
	}

	if b.State() != BreakerClosed {
		t.Errorf("state = %v; isolated failures must not open the breaker", b.State())
	}
}

func TestBreakerProbesAfterOpenWindow(t *testing.T) {
	clock := newFakeClock()
	b := newTestBreaker(clock)

	for i := 0; i < 3; i++ {
		b.Allow()
		b.Record(false)
	}
	if b.Allow() {
		t.Fatal("breaker should be open")
	}

	clock.advance(11 * time.Second)

	if !b.Allow() {
		t.Error("after the open window the breaker should admit a probe")
	}
	// The probe budget is one, so a second caller is still refused.
	if b.Allow() {
		t.Error("only HalfOpenProbes requests should be admitted while half-open")
	}
}

func TestBreakerClosesOnSuccessfulProbe(t *testing.T) {
	clock := newFakeClock()
	b := newTestBreaker(clock)

	for i := 0; i < 3; i++ {
		b.Allow()
		b.Record(false)
	}
	clock.advance(11 * time.Second)

	b.Allow()
	b.Record(true)

	if b.State() != BreakerClosed {
		t.Errorf("state = %v after a successful probe, want closed", b.State())
	}
	if !b.Allow() {
		t.Error("a closed breaker should admit requests")
	}
}

// A failed probe means the dependency has not recovered; continuing to probe would be a slow trickle
// of doomed requests against something already struggling.
func TestBreakerReopensOnFailedProbe(t *testing.T) {
	clock := newFakeClock()
	b := newTestBreaker(clock)

	for i := 0; i < 3; i++ {
		b.Allow()
		b.Record(false)
	}
	clock.advance(11 * time.Second)

	b.Allow()
	b.Record(false)

	if b.Allow() {
		t.Error("a failed probe must reopen the breaker for another full window")
	}

	clock.advance(11 * time.Second)
	if !b.Allow() {
		t.Error("the breaker should probe again after the second window")
	}
}

func TestBreakerReset(t *testing.T) {
	b := newTestBreaker(newFakeClock())
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Record(false)
	}

	b.Reset()

	if b.State() != BreakerClosed || !b.Allow() {
		t.Error("Reset should return the breaker to closed")
	}
}

func TestBreakerIsConcurrencySafe(t *testing.T) {
	b := newTestBreaker(SystemClock())

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if b.Allow() {
					b.Record(n%2 == 0)
				}
				_ = b.State()
			}
		}(i)
	}
	wg.Wait() // The race detector is the assertion here.
}
