package resilient

import (
	"math/rand"
	"testing"
	"time"
)

func TestExponentialBackoffDoubles(t *testing.T) {
	b := &ExponentialBackoff{Base: 100 * time.Millisecond, Max: time.Minute, Jitter: JitterNone}
	rnd := rand.New(rand.NewSource(1))

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
	}
	for i, w := range want {
		if got := b.Delay(i+1, rnd); got != w {
			t.Errorf("Delay(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestExponentialBackoffRespectsCap(t *testing.T) {
	b := &ExponentialBackoff{Base: 100 * time.Millisecond, Max: time.Second, Jitter: JitterNone}
	rnd := rand.New(rand.NewSource(1))

	if got := b.Delay(4, rnd); got != 800*time.Millisecond {
		t.Errorf("Delay(4) = %v, want 800ms", got)
	}
	for _, attempt := range []int{5, 10, 50} {
		if got := b.Delay(attempt, rnd); got != time.Second {
			t.Errorf("Delay(%d) = %v, want the 1s cap", attempt, got)
		}
	}
}

// A left shift by a large amount overflows silently into a negative duration, which would then be
// treated as "no delay" and turn backoff into a hot loop.
func TestExponentialBackoffSurvivesAbsurdAttemptCounts(t *testing.T) {
	b := &ExponentialBackoff{Base: time.Second, Max: 5 * time.Minute, Jitter: JitterNone}
	rnd := rand.New(rand.NewSource(1))

	for _, attempt := range []int{60, 63, 64, 1000, 1 << 30} {
		got := b.Delay(attempt, rnd)
		if got != 5*time.Minute {
			t.Errorf("Delay(%d) = %v, want the 5m cap", attempt, got)
		}
	}
}

func TestJitterStaysInBounds(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	cap := 10 * time.Second

	for _, j := range []Jitter{JitterNone, JitterFull, JitterEqual} {
		b := &ExponentialBackoff{Base: 50 * time.Millisecond, Max: cap, Jitter: j}
		for attempt := 1; attempt <= 30; attempt++ {
			got := b.Delay(attempt, rnd)
			if got < 0 || got > cap {
				t.Errorf("jitter %v attempt %d: delay %v outside [0, %v]", j, attempt, got, cap)
			}
		}
	}
}

// The reason jitter exists: many callers failing at once must not come back at once.
func TestFullJitterSpreadsRetries(t *testing.T) {
	b := &ExponentialBackoff{Base: time.Second, Max: 10 * time.Second, Jitter: JitterFull}
	rnd := rand.New(rand.NewSource(7))

	seen := make(map[time.Duration]bool)
	for i := 0; i < 200; i++ {
		// The same attempt number every time: 200 callers that failed simultaneously.
		seen[b.Delay(5, rnd)] = true
	}
	if len(seen) < 150 {
		t.Errorf("full jitter produced only %d distinct delays from 200 callers; "+
			"the herd is not being broken up", len(seen))
	}
}

func TestNoJitterIsPerfectlySynchronised(t *testing.T) {
	b := &ExponentialBackoff{Base: time.Second, Max: 10 * time.Second, Jitter: JitterNone}
	rnd := rand.New(rand.NewSource(7))

	seen := make(map[time.Duration]bool)
	for i := 0; i < 50; i++ {
		seen[b.Delay(3, rnd)] = true
	}
	if len(seen) != 1 {
		t.Errorf("JitterNone produced %d distinct delays, want exactly 1 "+
			"(this is the thundering herd, demonstrated)", len(seen))
	}
}

func TestEqualJitterNeverReturnsImmediately(t *testing.T) {
	b := &ExponentialBackoff{Base: 4 * time.Second, Max: 4 * time.Second, Jitter: JitterEqual}
	rnd := rand.New(rand.NewSource(7))

	for i := 0; i < 200; i++ {
		got := b.Delay(1, rnd)
		if got < 2*time.Second || got > 4*time.Second {
			t.Fatalf("equal jitter returned %v, want [2s, 4s]", got)
		}
	}
}

func TestConstantAndNoBackoff(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))

	c := ConstantBackoff{Delay_: 250 * time.Millisecond}
	if got := c.Delay(1, rnd); got != 250*time.Millisecond {
		t.Errorf("ConstantBackoff = %v, want 250ms", got)
	}
	if got := c.Delay(9, rnd); got != 250*time.Millisecond {
		t.Errorf("ConstantBackoff at attempt 9 = %v, want 250ms", got)
	}
	if got := (NoBackoff{}).Delay(3, rnd); got != 0 {
		t.Errorf("NoBackoff = %v, want 0", got)
	}
}
