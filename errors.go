package resilient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// ErrAttemptTimeout marks a failure caused by this package's own per-attempt timeout, as opposed to
// the caller's deadline.
//
// The distinction is essential and easy to miss, because both surface as context.DeadlineExceeded.
// They mean opposite things:
//
//   - The caller's deadline expiring means there is no time left to retry into. Stop.
//   - A per-attempt timeout expiring means this attempt was too slow and should be abandoned in
//     favour of another one. That is the entire reason to configure a per-attempt timeout.
//
// Without a way to tell them apart, a per-attempt timeout is self-defeating: it cuts the slow attempt
// short and is then classified as "the caller gave up", so no retry ever happens. Wrapping with this
// sentinel is what lets the retrier make the right call, and lets callers do the same with errors.Is.
var ErrAttemptTimeout = errors.New("resilient: attempt timeout exceeded")

// attemptTimeoutError wraps a transport error that this package's per-attempt timeout caused.
type attemptTimeoutError struct{ err error }

func (e *attemptTimeoutError) Error() string {
	return fmt.Sprintf("resilient: attempt timed out: %v", e.err)
}

// Unwrap exposes both the sentinel and the original error, so errors.Is finds ErrAttemptTimeout and
// callers can still reach the underlying *url.Error if they need it.
func (e *attemptTimeoutError) Unwrap() []error { return []error{ErrAttemptTimeout, e.err} }

// classifyAttemptError decides whether a failed attempt hit this package's per-attempt timeout rather
// than the caller's deadline, and tags it accordingly.
//
// The test is which context is actually done. The per-attempt context is derived from the caller's,
// so if the attempt failed with a deadline error while the caller's own context is still healthy, the
// deadline that fired must have been ours.
func classifyAttemptError(callerCtx context.Context, err error) error {
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if callerCtx.Err() != nil {
		// The caller's deadline or cancellation is what fired. Leave it alone.
		return err
	}
	return &attemptTimeoutError{err: err}
}

// StatusError reports a response whose status was retryable but which ran out of attempts.
//
// Not returned by default — RoundTrip hands back the final response so callers can inspect it, which
// is what an http.RoundTripper is expected to do. Provided for callers who would rather branch on an
// error than on a status code.
type StatusError struct {
	StatusCode int
	Status     string
	Attempts   int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("resilient: gave up after %d attempt(s); last status %s", e.Attempts, e.Status)
}

// StatusErrorFrom builds a StatusError from a response.
func StatusErrorFrom(resp *http.Response, attempts int) *StatusError {
	if resp == nil {
		return &StatusError{Attempts: attempts}
	}
	return &StatusError{StatusCode: resp.StatusCode, Status: resp.Status, Attempts: attempts}
}
