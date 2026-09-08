package resilient

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestDefaultRetrierStatusCodes(t *testing.T) {
	tests := []struct {
		status int
		want   bool
	}{
		{http.StatusOK, false},
		{http.StatusBadRequest, false},          // the request is wrong; it will be wrong next time too
		{http.StatusUnauthorized, false},        // credentials will not fix themselves
		{http.StatusNotFound, false},            // it will still be absent
		{http.StatusConflict, false},            // a genuine conflict, not transient
		{http.StatusRequestTimeout, true},       // 408
		{http.StatusTooManyRequests, true},      // 429
		{http.StatusInternalServerError, false}, // opt-in only; see the RetryOn500 test
		{http.StatusNotImplemented, false},      // 501 will never start working
		{http.StatusBadGateway, true},           // 502
		{http.StatusServiceUnavailable, true},   // 503
		{http.StatusGatewayTimeout, true},       // 504
	}

	r := DefaultRetrier{}
	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)

	for _, tc := range tests {
		resp := &http.Response{StatusCode: tc.status}
		if got := r.ShouldRetry(req, resp, nil); got != tc.want {
			t.Errorf("status %d: ShouldRetry = %v, want %v", tc.status, got, tc.want)
		}
	}
}

// The single most important safety property: a POST that timed out may have been fully processed,
// and the client cannot tell. Retrying it can charge a card twice.
func TestDefaultRetrierRefusesNonIdempotentMethods(t *testing.T) {
	r := DefaultRetrier{}
	resp := &http.Response{StatusCode: http.StatusServiceUnavailable}

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut,
		http.MethodDelete, http.MethodOptions, http.MethodTrace} {
		req, _ := http.NewRequest(method, "https://example.com", nil)
		if !r.ShouldRetry(req, resp, nil) {
			t.Errorf("%s is idempotent and should be retried", method)
		}
	}

	for _, method := range []string{http.MethodPost, http.MethodPatch} {
		req, _ := http.NewRequest(method, "https://example.com", nil)
		if r.ShouldRetry(req, resp, nil) {
			t.Errorf("%s is not idempotent and must not be retried by default", method)
		}
	}
}

func TestRetryNonIdempotentOptIn(t *testing.T) {
	r := DefaultRetrier{RetryNonIdempotent: true}
	req, _ := http.NewRequest(http.MethodPost, "https://example.com", nil)
	resp := &http.Response{StatusCode: http.StatusServiceUnavailable}

	if !r.ShouldRetry(req, resp, nil) {
		t.Error("RetryNonIdempotent should allow retrying POST")
	}
}

// 500 differs from 502/503/504: those say the request never reached a healthy backend, while a 500
// usually means it was processed and the handler blew up partway through.
func TestRetryOn500IsOptIn(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
	resp := &http.Response{StatusCode: http.StatusInternalServerError}

	if (DefaultRetrier{}).ShouldRetry(req, resp, nil) {
		t.Error("500 should not be retried by default")
	}
	if !(DefaultRetrier{RetryOn500: true}).ShouldRetry(req, resp, nil) {
		t.Error("RetryOn500 should allow retrying 500")
	}
}

func TestErrorClassification(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
	r := DefaultRetrier{}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"context canceled: the caller gave up", context.Canceled, false},
		{"deadline exceeded: no time left to retry into", context.DeadlineExceeded, false},
		{"connection refused", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{"dns temporary failure", &net.DNSError{IsTemporary: true}, true},
		{"dns nxdomain: will never resolve", &net.DNSError{IsNotFound: true}, false},
		{"opaque error: deterministic, so pointless to repeat", errors.New("bad protocol scheme"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.ShouldRetry(req, nil, tc.err); got != tc.want {
				t.Errorf("ShouldRetry(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Wrapped errors must still classify correctly; errors.Is/As is the whole reason to use them.
func TestErrorClassificationUnwraps(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
	wrapped := &url_Error{Err: context.Canceled}

	if (DefaultRetrier{}).ShouldRetry(req, nil, wrapped) {
		t.Error("a wrapped context.Canceled must still be recognised as non-retryable")
	}
}

// url_Error stands in for *url.Error, which is what net/http actually wraps transport errors in.
type url_Error struct{ Err error }

func (e *url_Error) Error() string { return "url error: " + e.Err.Error() }
func (e *url_Error) Unwrap() error { return e.Err }

func TestRetryAfterParsing(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		header string
		want   time.Duration
		ok     bool
	}{
		{"absent", "", 0, false},
		{"delta seconds", "120", 2 * time.Minute, true},
		{"zero seconds", "0", 0, true},
		{"negative is ignored", "-5", 0, false},
		{"http date in the future", "Mon, 01 Jan 2024 12:00:30 GMT", 30 * time.Second, true},
		{"http date in the past means now", "Mon, 01 Jan 2024 11:59:00 GMT", 0, true},
		{"garbage is ignored, not treated as zero", "soon please", 0, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{Header: http.Header{}}
			if tc.header != "" {
				resp.Header.Set("Retry-After", tc.header)
			}
			got, ok := retryAfter(resp, now)
			if ok != tc.ok || got != tc.want {
				t.Errorf("retryAfter(%q) = (%v, %v), want (%v, %v)", tc.header, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// Regression test. Both the caller's deadline and this package's per-attempt timeout surface as
// context.DeadlineExceeded. An earlier version classified all of them as "the caller gave up", which
// made per-attempt timeouts self-defeating: the timeout cut the slow attempt short and then forbade
// the retry it existed to enable.
func TestPerAttemptTimeoutIsRetryableButCallerDeadlineIsNot(t *testing.T) {
	t.Run("our per-attempt timeout is retried", func(t *testing.T) {
		err := classifyAttemptError(context.Background(), context.DeadlineExceeded)
		if !errors.Is(err, ErrAttemptTimeout) {
			t.Fatalf("error = %v, want it to wrap ErrAttemptTimeout", err)
		}
		if !isRetryableError(err) {
			t.Error("a per-attempt timeout must be retryable; that is the point of having one")
		}
	})

	t.Run("the caller's deadline is not retried", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		<-ctx.Done()

		err := classifyAttemptError(ctx, context.DeadlineExceeded)
		if errors.Is(err, ErrAttemptTimeout) {
			t.Error("the caller's own deadline must not be mistaken for a per-attempt timeout")
		}
		if isRetryableError(err) {
			t.Error("there is no time left to retry into once the caller's deadline has passed")
		}
	})
}
