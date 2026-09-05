package resilient

import (
	"math"
	"math/rand"
	"time"
)

// Backoff computes how long to wait before attempt n+1, given that attempt n has just failed.
//
// attempt counts from 1, so the delay after the first failure is Delay(1).
type Backoff interface {
	Delay(attempt int, rnd *rand.Rand) time.Duration
}

// BackoffFunc adapts a plain function to Backoff.
type BackoffFunc func(attempt int, rnd *rand.Rand) time.Duration

// Delay implements Backoff.
func (f BackoffFunc) Delay(attempt int, rnd *rand.Rand) time.Duration { return f(attempt, rnd) }

// Jitter controls how much randomness is mixed into a backoff delay.
//
// This is not a detail. When a dependency starts failing, every caller retries; if they all retry on
// the same schedule they come back at the same instant, and a dependency that might have recovered
// under a trickle is knocked over again by a synchronised wave. Jitter is what breaks that up.
//
// The names and behaviour follow the analysis in AWS's "Exponential Backoff and Jitter", which
// measured these against each other rather than reasoning about them.
type Jitter int

const (
	// JitterNone waits exactly the computed delay.
	//
	// Useful for tests and for demonstrating the problem. In production it is what produces
	// synchronised retry waves.
	JitterNone Jitter = iota

	// JitterFull draws uniformly from [0, delay].
	//
	// The default, and the best general-purpose choice. It spreads retries maximally, which both
	// minimises collisions and reduces total work done — callers that draw a short delay discover
	// recovery sooner, so the rest never retry at all.
	//
	// The objection is that a caller can draw a near-zero delay and come back immediately. In
	// aggregate that does not matter: the population is spread, which is the property that protects
	// the dependency.
	JitterFull

	// JitterEqual draws uniformly from [delay/2, delay].
	//
	// A compromise: still spread, but with a floor, so no caller retries instantly. Measurably
	// slightly worse than JitterFull on contention, and the right choice when each attempt is itself
	// expensive enough that a guaranteed minimum wait matters more.
	JitterEqual
)

// apply returns the delay to actually wait, given the un-jittered ceiling.
func (j Jitter) apply(ceiling time.Duration, rnd *rand.Rand) time.Duration {
	if ceiling <= 0 {
		return 0
	}
	switch j {
	case JitterFull:
		return time.Duration(rnd.Int63n(int64(ceiling) + 1))
	case JitterEqual:
		half := ceiling / 2
		return half + time.Duration(rnd.Int63n(int64(ceiling-half)+1))
	default:
		return ceiling
	}
}

// ExponentialBackoff doubles the delay per attempt, capped, with jitter applied.
//
// The right default for anything that talks to a network: the delay grows quickly so a persistently
// failing dependency is left alone, the cap stops it growing to something absurd, and the jitter
// spreads the retries out.
type ExponentialBackoff struct {
	// Base is the delay after the first failure. Defaults to 100ms if zero.
	Base time.Duration

	// Max caps the un-jittered delay. Defaults to 30s if zero.
	Max time.Duration

	// Jitter is how the delay is randomised. The zero value is JitterNone, so construct via
	// NewExponentialBackoff to get the JitterFull default rather than accidentally shipping
	// synchronised retries.
	Jitter Jitter
}

// NewExponentialBackoff returns exponential backoff with full jitter and sensible defaults:
// a 100ms base and a 30s cap.
func NewExponentialBackoff() *ExponentialBackoff {
	return &ExponentialBackoff{Base: 100 * time.Millisecond, Max: 30 * time.Second, Jitter: JitterFull}
}

// Delay implements Backoff.
func (b *ExponentialBackoff) Delay(attempt int, rnd *rand.Rand) time.Duration {
	base := b.Base
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	max := b.Max
	if max <= 0 {
		max = 30 * time.Second
	}
	if attempt < 1 {
		attempt = 1
	}

	// Shift rather than math.Pow: exact, and no float rounding. The exponent is clamped because
	// shifting by 63 or more is undefined, and anything beyond the cap is the cap regardless.
	ceiling := max
	if shift := attempt - 1; shift < 62 {
		grown := int64(base) << uint(shift)
		// A left shift overflows silently into a negative number; check rather than trust it.
		if grown > 0 && grown < int64(max) {
			ceiling = time.Duration(grown)
		}
	}

	return b.Jitter.apply(ceiling, rnd)
}

// ConstantBackoff waits the same amount every time.
//
// Simple and predictable, and the classic thundering-herd generator: every failed caller comes back
// at the same instant. Give it a Jitter unless the callers are known to be few.
type ConstantBackoff struct {
	Delay_ time.Duration
	Jitter Jitter
}

// Delay implements Backoff.
func (b ConstantBackoff) Delay(attempt int, rnd *rand.Rand) time.Duration {
	return b.Jitter.apply(b.Delay_, rnd)
}

// NoBackoff retries immediately.
//
// Appropriate only when the failure cannot be caused by load — a lost optimistic-locking race, say,
// where retrying at once against fresh state is exactly right. Never use it for a failing network
// call.
type NoBackoff struct{}

// Delay implements Backoff.
func (NoBackoff) Delay(int, *rand.Rand) time.Duration { return 0 }

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
