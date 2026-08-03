package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoSucceedsAfterRetries(t *testing.T) {
	t.Parallel()
	n := 0
	err := Do(context.Background(), 4, time.Millisecond, 10*time.Millisecond, func(attempt int) error {
		n++
		if n < 3 {
			return TemporaryError{Err: errors.New("flaky")}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("attempts=%d", n)
	}
}

func TestDoRespectsContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Do(ctx, 5, time.Second, time.Second, func(int) error {
		return TemporaryError{Err: errors.New("x")}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
