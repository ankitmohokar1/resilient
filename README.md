# resilient

An `http.RoundTripper` for Go that retries failed requests with jittered backoff, honours
`Retry-After`, and trips a circuit breaker when a dependency is consistently down.

![Go](https://img.shields.io/badge/Go-1.22%2B-00ADD8)
![License](https://img.shields.io/badge/License-Apache%202.0-blue)

> **Work in progress.** Backoff and the clock abstraction land first; retry classification, the
> circuit breaker and the transport itself follow. See the commit history.

No dependencies beyond the standard library.

## Why a RoundTripper

It composes. A `RoundTripper` drops into any existing `http.Client`, stacks with other middleware
(tracing, auth, metrics), and works unchanged with every library that accepts an `*http.Client`.
Wrapping the client instead forces every caller to adopt a bespoke type.

## Jitter

When a dependency starts failing, every caller retries. If they all retry on the same schedule they
come back at the same instant, and something that might have recovered under a trickle is knocked over
again by a synchronised wave.

| | Delay drawn from | Use when |
|---|---|---|
| `JitterFull` | `[0, delay]` | **Default.** Maximum spread, lowest total work. |
| `JitterEqual` | `[delay/2, delay]` | You need a guaranteed minimum wait. |
| `JitterNone` | exactly `delay` | Tests, and demonstrating the problem. |

## Testing

Time is injected — both `Now()` and `Sleep()`. That second half is usually skipped, and it is what
keeps the suite fast: a fake clock records what it was asked to wait for instead of waiting, so a
five-attempt backoff with a 30-second cap runs instantly and the delays become an exact assertion.

```bash
go test -race ./...
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
