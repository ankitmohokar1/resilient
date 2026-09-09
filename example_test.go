package resilient_test

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ankitmohokar1/resilient"
)

// The common case: drop it into an http.Client and carry on.
func Example() {
	client := &http.Client{
		Transport: resilient.New(
			resilient.WithMaxAttempts(4),
			resilient.WithPerAttemptTimeout(5*time.Second),
		),
		Timeout: 30 * time.Second,
	}

	resp, err := client.Get("https://example.com/api/thing")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	fmt.Println(resp.StatusCode)
}

// Retrying a POST is only safe when the server deduplicates — typically because you send an
// idempotency key. Opting in is deliberate for exactly that reason.
func Example_nonIdempotent() {
	client := &http.Client{
		Transport: resilient.New(
			resilient.WithRetrier(resilient.DefaultRetrier{RetryNonIdempotent: true}),
			resilient.WithMaxAttempts(3),
		),
	}

	req, _ := http.NewRequest(http.MethodPost, "https://example.com/payments", nil)
	req.Header.Set("Idempotency-Key", "order-1234")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
}

// A breaker stops sending requests to a dependency that is consistently failing, so it gets a chance
// to recover instead of being held down by retries.
func ExampleCircuitBreaker() {
	breaker := &resilient.CircuitBreaker{
		FailureThreshold: 5,
		OpenDuration:     10 * time.Second,
		HalfOpenProbes:   1,
	}

	client := &http.Client{
		Transport: resilient.New(
			resilient.WithBreaker(breaker),
			resilient.WithMaxAttempts(3),
		),
	}

	if _, err := client.Get("https://example.com/api"); err != nil {
		// Distinguishing "we did not even try" from "we tried and it failed" is often worth doing:
		// they are different operational situations.
		if err == resilient.ErrCircuitOpen {
			fmt.Println("dependency is down; serving from cache")
		}
	}

	fmt.Println(breaker.State())
}

// Backoff and jitter are configurable; full jitter is the default and usually the right choice.
func ExampleExponentialBackoff() {
	client := &http.Client{
		Transport: resilient.New(
			resilient.WithBackoff(&resilient.ExponentialBackoff{
				Base:   50 * time.Millisecond,
				Max:    5 * time.Second,
				Jitter: resilient.JitterFull,
			}),
		),
	}
	_ = client
}

// The retry hook is where metrics belong.
func ExampleWithOnRetry() {
	client := &http.Client{
		Transport: resilient.New(
			resilient.WithOnRetry(func(a resilient.Attempt) {
				log.Printf("retrying %s %s after attempt %d (waiting %v)",
					a.Request.Method, a.Request.URL.Path, a.Number, a.Delay)
			}),
		),
	}
	_ = client
}
