package notify

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	defaultConcurrency = 10
	defaultSendTimeout = 1 * time.Second
)

// Error is the type of every error returned by this package.
// Callers can use [errors.As] to check whether an error originated from
// notify, and [errors.Is] or [errors.Unwrap] to inspect an underlying cause
// such as [context.Canceled].
type Error interface {
	error
	Unwrap() error

	// isNotifyError marks types that originate from this package.
	isNotifyError()
}

type notifyError struct {
	msg string
	err error
}

// newError returns a new [Error] with the given message and an optional
// wrapped cause. It panics if `m` is empty and `e` is nil, since an error
// that conveys nothing is a programming mistake.
func newError(m string, e error) Error {
	if m == "" && e == nil {
		panic("silent errors are forbidden")
	}
	return &notifyError{msg: m, err: e}
}

func (e *notifyError) Error() string {
	switch {
	case e.err == nil:
		return e.msg
	case e.msg == "":
		return e.err.Error()
	default:
		return e.msg + ": " + e.err.Error()
	}
}

func (e *notifyError) Unwrap() error { return e.err }

func (e *notifyError) isNotifyError() {}

var (
	ErrListenerCanceled = newError("listener is canceled", nil)
	ErrNilContext       = newError("context is required", nil)
	ErrDuplicateID      = newError("duplicate listener ID", nil)
)

// Listener represents a registered listener that can receive notifications.
type Listener struct {
	cancel   func()
	canceled bool
	ch       chan struct{}
	ctx      context.Context
	id       string
	mu       sync.RWMutex
}

// ID returns the listener's unique identifier.
func (l *Listener) ID() string {
	return l.id
}

// Channel returns the receive-only notification channel.
func (l *Listener) Channel() <-chan struct{} {
	return l.ch
}

// IsCanceled reports whether the listener has been canceled.
func (l *Listener) IsCanceled() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.canceled
}

// Cancel removes the listener from the manager and closes its channel.
// It is safe to call multiple times.
func (l *Listener) Cancel() {
	if l.cancel == nil {
		return
	}

	l.cancel()
}

func (l *Listener) notify(ctx context.Context) error {
	// The lock is held for the entire duration of the send, including the blocking select.
	// This is intentional: `Cancel()` must wait for any in-flight `notify()` to complete before closing the channel,
	// ensuring no send happens on a closed channel.
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.canceled {
		return newError(fmt.Sprintf("notify listener %q: listener is canceled", l.id), ErrListenerCanceled)
	}

	select {
	case <-ctx.Done():
		return newError(fmt.Sprintf("notify listener %q: sender context is canceled", l.id), context.Cause(ctx))
	case <-l.ctx.Done():
		return newError(fmt.Sprintf("notify listener %q: listener context is canceled", l.id), context.Cause(l.ctx))
	case l.ch <- struct{}{}:
		return nil
	}
}

// Manager manages multiple listeners and provides a mechanism to notify them concurrently.
type Manager struct {
	listeners map[string]*Listener
	mu        sync.RWMutex
	sendSlots chan struct{}
}

// NewManager creates a new Manager with the given concurrency limit.
// If concurrency is 0 or negative, defaultConcurrency is used.
func NewManager(concurrency int) *Manager {
	sm := &Manager{
		listeners: make(map[string]*Listener),
	}

	if concurrency > 0 {
		sm.sendSlots = make(chan struct{}, concurrency)
	} else {
		sm.sendSlots = make(chan struct{}, defaultConcurrency)
	}

	return sm
}

// NewListener registers a new listener with the manager and returns it.
// If an empty string is provided as the ID, a new UUID will be generated.
// The listener will be automatically canceled when the provided context is canceled.
func (m *Manager) NewListener(ctx context.Context, id string) (*Listener, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if ctx.Err() != nil {
		return nil, newError("context is already canceled", context.Cause(ctx))
	}

	if id == "" {
		id = uuid.NewString()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.listeners[id]; exists {
		return nil, newError(fmt.Sprintf("register listener %q", id), ErrDuplicateID)
	}

	listenerCtx, cancel := context.WithCancel(ctx)

	newL := &Listener{id: id, ch: make(chan struct{}), ctx: listenerCtx}
	newL.cancel = func() {
		cancel()

		// Mark the listener as canceled and close its channel under the listener's lock.
		// This ensures that any in-flight `notify()` call completes before we
		// close the channel, preventing "send on closed channel" panics.
		newL.mu.Lock()
		if newL.canceled {
			newL.mu.Unlock()
			return
		}
		newL.canceled = true
		close(newL.ch)
		newL.mu.Unlock()

		// Remove the listener from the manager's map.
		// This is done under the manager's lock, separately from the listener's lock,
		// to avoid holding both locks simultaneously and to keep lock ordering simple.
		m.mu.Lock()
		delete(m.listeners, id)
		m.mu.Unlock()
	}
	m.listeners[id] = newL

	// Ensure the listener is cleaned up if the caller's context is canceled,
	// even if the caller never explicitly calls `Cancel()`.
	go func() {
		<-listenerCtx.Done()
		newL.Cancel()
	}()

	return newL, nil
}

// GetListenerByID returns the listener with the given ID, or false if not found.
func (m *Manager) GetListenerByID(id string) (*Listener, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	listener, ok := m.listeners[id]

	return listener, ok
}

// Notify sends a signal to all registered listeners concurrently.
// Returns per-listener results and a top-level error if the operation context is canceled.
func (m *Manager) Notify(ctx context.Context, timeout time.Duration) (results map[string]error, err error) {
	if ctx == nil {
		err = ErrNilContext
		return
	}
	if ctx.Err() != nil {
		err = newError("context is already canceled", context.Cause(ctx))
		return
	}

	results = make(map[string]error)
	if timeout <= 0 {
		timeout = defaultSendTimeout
	}

	// Snapshot currently registered listeners.
	m.mu.RLock()
	listeners := make([]*Listener, 0, len(m.listeners))
	for _, l := range m.listeners {
		listeners = append(listeners, l)
	}
	m.mu.RUnlock()

	resultsCh := make(chan struct {
		id  string
		err error
	}, len(listeners))
	senders := new(sync.WaitGroup)

	// Send a signal to every registered listener.
sendingLoop:
	for _, l := range listeners {
		copyL := l
		// Notify concurrently but limit the number of concurrent sends.
		select {
		case <-ctx.Done():
			err = newError("notify listeners: operation context is canceled", context.Cause(ctx))
			break sendingLoop
		case m.sendSlots <- struct{}{}:
			// Re-check the context before dispatching. The select above may
			// have committed to this case even though ctx.Done was ready.
			if ctx.Err() != nil {
				<-m.sendSlots
				err = newError("notify listeners: operation context is canceled", context.Cause(ctx))
				break sendingLoop
			}

			sendCtx, cancel := context.WithTimeout(ctx, timeout)
			senders.Add(1)
			go func() {
				defer senders.Done()
				defer cancel()
				resultsCh <- struct {
					id  string
					err error
				}{copyL.id, copyL.notify(sendCtx)}
				<-m.sendSlots
			}()
		}
	}

	senders.Wait()
	close(resultsCh)
	for r := range resultsCh {
		results[r.id] = r.err
	}
	return
}
