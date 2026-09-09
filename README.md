# resilient

An `http.RoundTripper` for Go that retries failed requests with jittered backoff, honours
`Retry-After`, and trips a circuit breaker when a dependency is consistently down.

[![CI](https://github.com/ankitmohokar1/resilient/actions/workflows/ci.yml/badge.svg)](https://github.com/ankitmohokar1/resilient/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ankitmohokar1/resilient.svg)](https://pkg.go.dev/github.com/ankitmohokar1/resilient)
![Go](https://img.shields.io/badge/Go-1.22%2B-00ADD8)
![License](https://img.shields.io/badge/License-Apache%202.0-blue)

```go
client := &http.Client{
    Transport: resilient.New(
        resilient.WithMaxAttempts(4),
        resilient.WithPerAttemptTimeout(5*time.Second),
    ),
}

resp, err := client.Get("https://example.com/api")
```

No dependencies beyond the standard library.

---

## Why a RoundTripper

It composes. A `RoundTripper` drops into any existing `http.Client`, stacks with other middleware
(tracing, auth, metrics), and works unchanged with every library that accepts an `*http.Client`.
Wrapping the client instead — the more common approach — forces every caller to adopt a bespoke type
and stops the retry layer being combined with anything else.

Ordering follows from that: middleware placed *beneath* this one sees every individual attempt, which
is what you want for tracing and metrics.

## The four things that are easy to get wrong

### 1. Retrying a request that was already processed

A failed request has three possible fates, and the client cannot distinguish them: it never reached
the server; it reached the server and was rejected; or it reached the server, was **fully processed**,
and the response was lost coming back.

Retrying is safe only if the third case is harmless — which is exactly what *idempotent* means. So
`POST` and `PATCH` are **not retried by default**:

```go
// Safe: GET, HEAD, PUT, DELETE, OPTIONS, TRACE are retried.
// POST and PATCH are not, unless you opt in:
resilient.WithRetrier(resilient.DefaultRetrier{RetryNonIdempotent: true})
```

Opt in when the server deduplicates — typically because you send an idempotency key. Retrying a
payment POST that timed out can charge a card twice, and the client has no way to know it happened.

`500` is likewise opt-in (`RetryOn500`). Unlike `502`/`503`/`504`, which say the request never reached
a healthy backend, a `500` usually means it *was* processed and the handler blew up partway.

### 2. Retrying with a body that has already been read

A request body is an `io.ReadCloser`, and the first attempt consumes it. Retry naively and the second
attempt sends **an empty body** — the server sees a malformed request, and nothing anywhere reports an
error.

`net/http` populates `req.GetBody` for the rewindable body types (`bytes.Reader`, `bytes.Buffer`,
`strings.Reader`) precisely so redirects and HTTP/2 can replay them. This package uses it, so the
common cases work with no ceremony. A body that genuinely cannot be rewound — an open file, a
streaming upload — produces a clear error rather than a silent empty request:

```
resilient: cannot retry POST https://…: request body is not replayable (no GetBody).
Buffer the body yourself, or use a body type net/http can rewind
```

### 3. Dropping response bodies between attempts

`net/http` only returns a connection to the pool once its body is read to EOF **and** closed. A retry
loop that discards responses without draining them quietly exhausts the connection pool and then
stalls — a failure that looks exactly like the dependency being slow, which is where you will spend
your debugging time.

Every discarded response is drained (capped at 64 KiB) and closed. `TestDrainsDiscardedResponseBodies`
pins it down by allowing exactly one connection and requiring five attempts; a leak turns the test
into a timeout.

### 4. Confusing your timeout with the caller's

A per-attempt timeout and the caller's deadline both surface as `context.DeadlineExceeded`, and they
mean **opposite** things:

- the caller's deadline expiring → there is no time left to retry into, so stop
- a per-attempt timeout expiring → this attempt was too slow, abandon it and try another

This package got that wrong at first, and a test caught it: with all deadline errors classified as
"the caller gave up", the per-attempt timeout cut the slow attempt short and then forbade the retry it
existed to enable — self-defeating, and silent. Failures caused by our own timeout are now tagged with
`ErrAttemptTimeout`, distinguished by checking whether the *caller's* context is still healthy.

Worth setting a per-attempt timeout for the same reason: without one, a single hung attempt consumes
the whole budget and the remaining attempts never run.

## Jitter

When a dependency starts failing, every caller retries. If they all retry on the same schedule they
come back at the same instant, and something that might have recovered under a trickle is knocked over
again by a synchronised wave.

| | Delay drawn from | Use when |
|---|---|---|
| `JitterFull` | `[0, delay]` | **Default.** Maximum spread, lowest total work. |
| `JitterEqual` | `[delay/2, delay]` | You need a guaranteed minimum wait. |
| `JitterNone` | exactly `delay` | Tests, and demonstrating the problem. |

Following the analysis in AWS's *Exponential Backoff and Jitter*. The tests assert the difference
rather than describing it: 200 callers failing simultaneously produce >150 distinct delays under
`JitterFull`, and exactly **one** under `JitterNone`.

`Retry-After` overrides the computed backoff when a server sends it — a rate limiter knows exactly
when its budget refills, which no local calculation can. It is capped (30s by default), because an
unbounded `Retry-After` lets a server park your client for hours.

## Circuit breaker

```go
breaker := &resilient.CircuitBreaker{
    FailureThreshold: 5,
    OpenDuration:     10 * time.Second,
    HalfOpenProbes:   1,
}
client := &http.Client{Transport: resilient.New(resilient.WithBreaker(breaker))}
```

Off by default, since it changes failure behaviour in a way that should be deliberate: once open it
returns `ErrCircuitOpen` without attempting the request at all.

The point is **not** mainly to fail the caller faster. It is to stop adding load to something already
struggling, and to stop paying a connect timeout per request while it is unreachable.

Only *consecutive* failures count toward opening. A single timeout under load is noise, and a breaker
that trips on it flaps — which is worse than no breaker, because the behaviour becomes unpredictable
exactly when someone is trying to reason about an incident.

After the open window, a limited number of probes are admitted rather than reopening the floodgates:
slamming a just-recovered dependency with the full backlog is how a recovery becomes a second outage.

## Testing

```
57 tests · 85% coverage · race-clean · runs in under a second
```

Time is injected — both `Now()` and `Sleep()`. That second half is the one usually skipped, and it is
what keeps the suite fast: a fake clock records what it was asked to wait for instead of waiting, so a
five-attempt exponential backoff with a 30-second cap runs instantly and the delays become an exact
assertion:

```go
want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
if got := clock.sleeps(); !equal(got, want) { … }
```

Concurrency tests assert via `-race` rather than by inspection — the shared `*rand.Rand` behind jitter
is the obvious hazard, and CI runs `go test -race`.

## Not implemented

- **Hedged requests.** Sending a duplicate after p99 and taking the first response cuts tail latency,
  but it multiplies load and is only safe for idempotent requests. Worth adding; deliberately out of
  scope for a first version.
- **Adaptive concurrency / load shedding.** A different problem (how many in flight, not what to do
  when one fails) and it belongs in its own layer.
- **Per-host breaker state.** One breaker covers one `Transport`. Pointing a client at many hosts
  wants a breaker per host; construct a Transport per host until this exists.

## License

Apache 2.0 — see [LICENSE](LICENSE).
