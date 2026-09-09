package resilient

import (
	"io"
	"sync"
)

// cancelOnCloseBody wraps a response body so that closing it also releases the attempt's context.
//
// See bindCancelToBody for why the cancellation has to be deferred this far.
type cancelOnCloseBody struct {
	io.ReadCloser
	cancel func()
	once   sync.Once
}

// Close closes the underlying body and then cancels the attempt context.
//
// sync.Once because Close may legitimately be called more than once — net/http does it in some
// paths, and defensive callers do it too — and calling a context's cancel twice is harmless but
// closing an underlying body twice is not guaranteed to be.
func (b *cancelOnCloseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.cancel)
	return err
}
