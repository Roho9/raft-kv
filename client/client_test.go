package client

import (
	"context"
	"testing"
	"time"
)

// TestCallRespectsContextCancellation guards against the retry loop
// ignoring the caller's context during its between-attempt pause. Every
// attempt here fails immediately (nothing is listening on the target
// address), so with a short-lived context the call must return as soon as
// the context is done, not after it burns through the full retry budget
// (maxAttempts * retryPause, 2s).
func TestCallRespectsContextCancellation(t *testing.T) {
	c, err := New([]string{"127.0.0.1:1"}) // nothing listens here
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, err = c.Get(ctx, "key")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error (context deadline), got nil")
	}
	if elapsed > time.Second {
		t.Fatalf("Get took %s after a 150ms context timeout; retry pause is not respecting ctx cancellation", elapsed)
	}
}
