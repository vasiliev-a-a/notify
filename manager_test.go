package notify

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewManager(t *testing.T) {
	t.Run("Default Settings", func(t *testing.T) {
		m := NewManager(0)

		if m.listeners == nil {
			t.Fatal("expected `listeners` map to be initialized")
		}
		if m.sendSlots == nil {
			t.Fatal("expected `sendSlots` channel to be initialized")
		}
		if cap(m.sendSlots) != defaultConcurrency {
			t.Fatalf("expected `sendSlots` channel capacity to be %d, got %d", defaultConcurrency, cap(m.sendSlots))
		}
	})

	t.Run("Custom Settings", func(t *testing.T) {
		customConcurrency := 20
		m := NewManager(customConcurrency)

		if m.listeners == nil {
			t.Fatal("expected `listeners` map to be initialized")
		}
		if m.sendSlots == nil {
			t.Fatal("expected `sendSlots` channel to be initialized")
		}
		if cap(m.sendSlots) != customConcurrency {
			t.Fatalf("expected `sendSlots` channel capacity to be %d, got %d", customConcurrency, cap(m.sendSlots))
		}
	})
}

func TestManager_NewListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	t.Run("Empty ID", func(t *testing.T) {
		m := NewManager(0)
		l, err := m.NewListener(ctx, "")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if l.Channel() == nil {
			t.Fatal("expected new listener channel to be initialized")
		}
		if l.IsCanceled() {
			t.Fatal("expected new listener to be not canceled")
		}
		if err := uuid.Validate(l.ID()); err != nil {
			t.Fatalf("expected generated ID to be a valid UUID, got %q: %v", l.ID(), err)
		}
		if l.cancel == nil {
			t.Fatal("expected cancel function to be initialized")
		}

		actual, ok := m.GetListenerByID(l.ID())

		if !ok {
			t.Fatal("expected returned listener to be stored in map")
		}
		if !reflect.DeepEqual(actual, l) {
			t.Fatal("returned listener is not the same as stored in map")
		}
	})

	t.Run("Custom ID", func(t *testing.T) {
		customID := "test-id"
		m := NewManager(0)
		l, err := m.NewListener(ctx, customID)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if l.Channel() == nil {
			t.Fatal("expected new listener channel to be initialized")
		}
		if l.IsCanceled() {
			t.Fatal("expected new listener to be not canceled")
		}
		if l.ID() != customID {
			t.Fatalf("expected ID to be %q, got %q", customID, l.ID())
		}
		if l.cancel == nil {
			t.Fatal("expected cancel function to be initialized")
		}

		actual, ok := m.GetListenerByID(l.ID())

		if !ok {
			t.Fatal("expected returned listener to be stored in map")
		}
		if !reflect.DeepEqual(actual, l) {
			t.Fatal("returned listener is not the same as stored in map")
		}
	})

	t.Run("Nil context", func(t *testing.T) {
		m := NewManager(0)
		//lint:ignore SA1012 passing a nil context is the condition under test
		_, err := m.NewListener(nil, "")

		if err == nil {
			t.Fatal("expected an error")
		}
		var e Error
		if !errors.As(err, &e) {
			t.Fatalf("expected `notify.Error`, got %T", err)
		}
		if !errors.Is(err, ErrNilContext) {
			t.Fatalf("expected %q error, got %q", ErrNilContext, err)
		}
	})

	t.Run("Duplicate ID", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		m := NewManager(0)
		_, err := m.NewListener(ctx, "duplicate-id")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = m.NewListener(ctx, "duplicate-id")
		if err == nil {
			t.Fatal("expected an error")
		}
		var e Error
		if !errors.As(err, &e) {
			t.Fatalf("expected `notify.Error`, got %T", err)
		}
		if !errors.Is(err, ErrDuplicateID) {
			t.Fatalf("expected %q error, got %q", ErrDuplicateID, err)
		}
	})
}

func TestListener_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	m := NewManager(0)
	l, err := m.NewListener(ctx, "")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	l.Cancel()

	if !l.IsCanceled() {
		t.Fatal("expected listener to be canceled")
	}
	if _, ok := <-l.Channel(); ok {
		t.Fatal("expected listener channel to be closed")
	}
}

func TestListener_CancelContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	m := NewManager(0)
	l, err := m.NewListener(ctx, "")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cancel()
	// A little delay to let scheduler process async shutdown.
	<-time.After(300 * time.Millisecond)

	if !l.IsCanceled() {
		t.Fatal("expected listener to be canceled")
	}
	if _, ok := <-l.Channel(); ok {
		t.Fatal("expected listener channel to be closed")
	}
}

func TestManager_Notify(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	timeout := 1 * time.Second
	// This many active senders are allowed at any moment.
	testConcurrency := 5

	t.Run("Normal", func(t *testing.T) {
		m := NewManager(testConcurrency)
		receivers := new(sync.WaitGroup)

		// Register multiple listeners
		for i := 0; i < testConcurrency; i++ {
			l, err := m.NewListener(ctx, "")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer l.Cancel()

			// Fire up receivers.
			receivers.Add(1)
			go func(ch <-chan struct{}) {
				defer receivers.Done()
				select {
				case <-ch:
					// Notification received
				case <-time.After(timeout):
					t.Errorf("expected to receive a notification from listener %q within timeout", l.ID())
				}
			}(l.Channel())

		}

		// Notify when all receivers are done.
		doneCh := make(chan struct{})
		go func() {
			receivers.Wait()
			close(doneCh)
		}()

		// Notify listeners and gather results.
		type data struct {
			results map[string]error
			err     error
		}
		dataCh := make(chan *data, 1)
		go func() {
			r, e := m.Notify(ctx, timeout-200*time.Millisecond)
			dataCh <- &data{err: e, results: r}
		}()

		select {
		case <-time.After(timeout + 200*time.Millisecond):
			t.Fatal("expected to complete notification within timeout")
		case <-doneCh:
			// All receivers completed.
			data := <-dataCh
			if data.err != nil {
				t.Fatalf("unexpected error: %v", data.err)
			}
			if len(data.results) != testConcurrency {
				t.Fatalf("expected %d results, got %d", testConcurrency, len(data.results))
			}
			for i, r := range data.results {
				if err := uuid.Validate(i); err != nil {
					t.Errorf("expected a valid UUID, got %q: %v", i, err)
				}
				if r != nil {
					t.Errorf("unexpected error: %v", r)
				}
			}
		}
	})

	t.Run("Timeout", func(t *testing.T) {
		m := NewManager(testConcurrency)

		// Register multiple listeners
		for i := 0; i < testConcurrency; i++ {
			l, err := m.NewListener(ctx, "")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer l.Cancel()
		}

		// Notify listeners.
		doneCh := make(chan struct{})
		type data struct {
			results map[string]error
			err     error
		}
		dataCh := make(chan *data, 1)
		go func() {
			r, e := m.Notify(ctx, timeout-200*time.Millisecond)
			dataCh <- &data{results: r, err: e}
			close(doneCh)
		}()

		select {
		case <-time.After(timeout + 200*time.Millisecond):
			t.Fatal("expected to complete notification within timeout")
		case <-doneCh:
			// Notification completed.
			data := <-dataCh
			if data.err != nil {
				t.Fatalf("unexpected error: %v", data.err)
			}
			if len(data.results) != testConcurrency {
				t.Fatalf("expected %d results, got %d", testConcurrency, len(data.results))
			}
			for i, r := range data.results {
				if err := uuid.Validate(i); err != nil {
					t.Errorf("expected a valid UUID, got %q: %v", i, err)
				}
				if r == nil {
					t.Error("expected an error")
				}
				var e Error
				if !errors.As(r, &e) {
					t.Fatalf("expected `notify.Error`, got %T", r)
				}
				if !errors.Is(r, context.DeadlineExceeded) {
					t.Errorf("expected error `context.DeadlineExceeded`, got: %v", r)
				}
			}
		}
	})

	t.Run("Canceled Notify Context", func(t *testing.T) {
		m := NewManager(testConcurrency)
		notifyCtx, notifyCtxCancel := context.WithCancel(ctx)
		time.AfterFunc(500*time.Millisecond, notifyCtxCancel)

		// Register multiple listeners
		// Extra one over allowed concurrency to trigger the [Manager.Notify] error.
		for i := 0; i < testConcurrency+1; i++ {
			l, err := m.NewListener(context.Background(), "")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer l.Cancel()
		}

		// Notify listeners.
		doneCh := make(chan struct{})
		type data struct {
			results map[string]error
			err     error
		}
		dataCh := make(chan *data, 1)
		go func() {
			r, e := m.Notify(notifyCtx, timeout-200*time.Millisecond)
			dataCh <- &data{results: r, err: e}
			close(doneCh)
		}()

		select {
		case <-time.After(timeout + 200*time.Millisecond):
			t.Fatal("expected to complete notification within timeout")
		case <-doneCh:
			data := <-dataCh
			if data.err == nil {
				t.Fatal("expected an error")
			}
			var e Error
			if !errors.As(data.err, &e) {
				t.Fatalf("expected `notify.Error`, got %T", data.err)
			}
			if !errors.Is(data.err, context.Canceled) {
				t.Fatalf("expected error `context.Canceled`, got: %v", data.err)
			}

			if len(data.results) > testConcurrency {
				t.Fatalf("expected at most %d results, got %d", testConcurrency, len(data.results))
			}
			for i, r := range data.results {
				if err := uuid.Validate(i); err != nil {
					t.Errorf("expected a valid UUID, got %q: %v", i, err)
				}
				if r == nil {
					t.Error("expected an error")
				}
				var e Error
				if !errors.As(r, &e) {
					t.Fatalf("expected `notify.Error`, got %T", r)
				}
				if !errors.Is(r, context.Canceled) {
					t.Errorf("expected error `context.Canceled`, got: %v", r)
				}
			}
		}
	})

	t.Run("Canceled Listener Context", func(t *testing.T) {
		m := NewManager(testConcurrency)
		listenerCtx, listenerCtxCancel := context.WithCancel(ctx)
		time.AfterFunc(500*time.Millisecond, listenerCtxCancel)

		// Register multiple listeners.
		for i := 0; i < testConcurrency; i++ {
			l, err := m.NewListener(listenerCtx, "")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer l.Cancel()
		}

		// Notify listeners.
		doneCh := make(chan struct{})
		type data struct {
			results map[string]error
			err     error
		}
		dataCh := make(chan *data, 1)
		go func() {
			r, e := m.Notify(context.Background(), timeout-200*time.Millisecond)
			dataCh <- &data{results: r, err: e}
			close(doneCh)
		}()

		select {
		case <-time.After(timeout + 200*time.Millisecond):
			t.Fatal("expected to complete notification within timeout")
		case <-doneCh:
			data := <-dataCh
			if data.err != nil {
				t.Fatal("unexpected error")
			}

			if len(data.results) > testConcurrency {
				t.Fatalf("expected at most %d results, got %d", testConcurrency, len(data.results))
			}
			for i, r := range data.results {
				if err := uuid.Validate(i); err != nil {
					t.Errorf("expected a valid UUID, got %q: %v", i, err)
				}
				if r == nil {
					t.Error("expected an error")
				}
				var e Error
				if !errors.As(r, &e) {
					t.Fatalf("expected `notify.Error`, got %T", r)
				}
				if !errors.Is(r, context.Canceled) && !errors.Is(r, ErrListenerCanceled) {
					t.Errorf("expected error `context.Canceled` or `ErrListenerCanceled`, got: %v", r)
				}
			}
		}
	})
}
