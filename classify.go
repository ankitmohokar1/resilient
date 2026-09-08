package resilient

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"
)

// Retrier decides whether a failed attempt should be retried.
//
// resp is nil when the transport itself failed; err is nil when a response came back but its status
// warrants a retry. Exactly one of them is non-nil.
type Retrier interface {
	ShouldRetry(req *http.Request, resp *http.Response, err error) bool
}

// RetrierFunc adapts a plain function to Retrier.
type RetrierFunc func(req *http.Request, resp *http.Response, err error) bool

// ShouldRetry implements Retrier.
func (f RetrierFunc) ShouldRetry(req *http.Request, resp *http.Response, err error) bool {
	return f(req, resp, err)
}

// DefaultRetrier is the policy used when none is configured.
//
// It is deliberately conservative about one thing above all: it will not retry a request whose method
// is not idempotent. See ShouldRetry for why that matters more than it looks.
type DefaultRetrier struct {
	// RetryNonIdempotent allows retrying POST and PATCH.
	//
	// Off by default. Turn it on only for endpoints you know deduplicate — typically because you send
	// an idempotency key. See ShouldRetry.
	RetryNonIdempotent bool

	// RetryOn500 treats a bare 500 as retryable.
	//
	// Off by default. Unlike 502/503/504, which say "the request did not reach a healthy backend",
	// a 500 usually means the request *was* processed and the handler blew up partway. Retrying that
	// repeats whatever side effects it managed before failing.
	RetryOn500 bool
}

// idempotentMethods are those RFC 9110 defines as idempotent: repeating them has the same effect on
// the server as making them once.
var idempotentMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodHead:    true,
	http.MethodPut:     true,
	http.MethodDelete:  true,
	http.MethodOptions: true,
	http.MethodTrace:   true,
}

// retryableStatus are the statuses that indicate a transient condition rather than a problem with
// the request itself.
var retryableStatus = map[int]bool{
	http.StatusRequestTimeout:      true, // 408
	http.StatusTooManyRequests:     true, // 429
	http.StatusBadGateway:          true, // 502
	http.StatusServiceUnavailable:  true, // 503
	http.StatusGatewayTimeout:      true, // 504
	http.StatusInsufficientStorage: true, // 507
}

// ShouldRetry implements Retrier.
//
// # Why idempotency gates everything
//
// A failed request has three possible fates, and from the client they are indistinguishable: it never
// reached the server; it reached the server and was rejected; or it reached the server, was fully
// processed, and the response was lost on the way back. Retrying is only safe if the third case is
// harmless — which is exactly what idempotent means.
//
// Retrying a POST that timed out can charge a card twice. The client cannot tell, so the default is
// to not retry it at all. Callers that send an idempotency key can set RetryNonIdempotent.
//
// # Why some errors are never retried
//
// A cancelled context means the caller has given up; retrying serves nobody and delays the caller's
// own shutdown. A malformed URL or an unsupported scheme will fail identically forever, so retrying
// converts one fast error into several slow ones.
func (d DefaultRetrier) ShouldRetry(req *http.Request, resp *http.Response, err error) bool {
	if req != nil && !d.methodAllowed(req.Method) {
		return false
	}

	if err != nil {
		return isRetryableError(err)
	}
	if resp == nil {
		return false
	}
	if resp.StatusCode == http.StatusInternalServerError {
		return d.RetryOn500
	}
	return retryableStatus[resp.StatusCode]
}

func (d DefaultRetrier) methodAllowed(method string) bool {
	if idempotentMethods[method] {
		return true
	}
	// An empty method means GET, per net/http.
	if method == "" {
		return true
	}
	return d.RetryNonIdempotent
}

// isRetryableError reports whether a transport-level error is worth another attempt.
func isRetryableError(err error) bool {
	// Checked before the deadline case below, because a per-attempt timeout also presents as
	// context.DeadlineExceeded and means the opposite thing: this attempt was too slow and should be
	// abandoned in favour of another. Ordering these the other way makes per-attempt timeouts
	// self-defeating, since the timeout would cut the attempt short and then forbid the retry.
	if errors.Is(err, ErrAttemptTimeout) {
		return true
	}

	// The caller gave up, or their deadline passed. Neither improves by trying again, and retrying
	// past a cancellation actively holds up the caller's shutdown.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Timeouts and explicitly temporary conditions are the canonical transient failures.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// Connection refused, connection reset, no route to host: the request did not reach a working
	// server, so repeating it is safe and likely to hit a different one behind a load balancer.
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		// A temporary resolver failure is worth retrying; NXDOMAIN will never resolve.
		return dnsErr.IsTemporary || dnsErr.IsTimeout
	}

	// Anything else — a malformed URL, an unsupported scheme, a TLS certificate that will not
	// validate — is deterministic. Retrying only turns one fast failure into several slow ones.
	return false
}

// retryAfter extracts a server-specified delay from a response, if it gave one.
//
// A server that sends Retry-After is telling the client exactly when to come back, and it knows more
// than any local backoff calculation does — a 429 from a rate limiter knows precisely when the
// budget refills. Honouring it is both more correct and more polite than guessing.
//
// RFC 9110 permits two forms: delta-seconds, and an HTTP-date. Both are handled; anything else is
// ignored rather than treated as zero, since a zero delay on a 429 is the worst possible response.
func retryAfter(resp *http.Response, now time.Time) (time.Duration, bool) {
	if resp == nil {
		return 0, false
	}
	value := resp.Header.Get("Retry-After")
	if value == "" {
		return 0, false
	}

	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}

	if at, err := http.ParseTime(value); err == nil {
		d := at.Sub(now)
		if d < 0 {
			// A date already in the past means "come back now".
			return 0, true
		}
		return d, true
	}

	return 0, false
}
