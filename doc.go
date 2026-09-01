// Package notify provides a concurrent notification fan-out mechanism.
//
// It allows a single sender to notify multiple registered listeners concurrently,
// with bounded parallelism and per-send timeouts. Each listener receives
// notifications on its own channel.
//
// # Quick Start
//
// Create a Manager, register listeners, and call Notify:
//
//	mgr := notify.NewManager(10) // up to 10 concurrent sends
//
//	l1, _ := mgr.NewListener(ctx, "")
//	l2, _ := mgr.NewListener(ctx, "custom-id")
//
//	// Consumers read from l1.Channel() and l2.Channel().
//
//	results, err := mgr.Notify(ctx, 500*time.Millisecond)
//
// # Core Types
//
// A [Manager] owns a set of [Listener] instances and coordinates concurrent
// notification delivery. A `Listener` is a registered subscriber that exposes
// a receive-only channel ([Listener.Channel]) and a cancellation method
// ([Listener.Cancel]). All errors returned by the package are of type
// [Error]; see Error Handling below.
//
// # Notification Semantics
//
// [Manager.Notify] sends a single `struct{}{}` to every currently-registered listener.
// Delivery is concurrent but bounded by the Manager's concurrency limit.
// Each individual send is subject to the timeout passed to Notify, which defaults to 1 second; if a
// listener is not actively receiving, its send will fail with
// [context.DeadlineExceeded] after the timeout elapses.
//
// Listener channels are unbuffered: a send only completes when a consumer
// is actively reading from the channel. This means slow or absent consumers
// will cause per-listener timeout errors without blocking other listeners.
// If a listener is canceled while a send to it is in flight, the per-listener
// error wraps the cause of the listener's context.
//
// # Error Handling
//
// Notify returns two values:
//
//   - A top-level error for operation-wide failures (e.g., a nil context or
//     the operation context being canceled mid-fan-out). If this is non-nil,
//     some listeners may not have been attempted.
//   - A per-listener error map keyed by the listener IDs, with one entry per listener that was
//     attempted. A nil entry means the notification was delivered
//     successfully; an absent key means no attempt was made; a non-nil entry contains a wrapped error
//     that can be inspected with [errors.Is].
//
// Sentinel errors are returned (wrapped) for conditions callers may want to
// inspect with `errors.Is`:
//
//   - [ErrListenerCanceled] when a listener was explicitly canceled via [Listener.Cancel]
//   - [ErrNilContext] when a nil context is passed to [Manager.NewListener] or [Manager.Notify]
//   - [ErrDuplicateID] when [Manager.NewListener] is called with an ID that is already
//     registered
//
// Every error returned by this package (including the sentinels above) is
// of type [Error]. Use [errors.As] to check whether an error originated from
// `notify`, and [errors.Is] or [errors.Unwrap] to inspect an underlying cause.
//
// # Listener Lifecycle and Cleanup
//
// Listeners can be removed in two ways:
//
//   - Explicitly, by calling [Listener.Cancel]. This closes the channel and
//     removes the listener from the [Manager]. Cancel is safe to call multiple
//     times.
//   - Implicitly, when the context passed to [Manager.NewListener] is canceled. The
//     Manager watches the listener's context and auto-cleans it up.
//
// In both cases, any in-flight notify call on that listener completes before
// the channel is closed, so consumers will never observe a "send on closed
// channel" panic.
//
// # Concurrency Safety
//
// Both [Manager] and [Listener] are safe for concurrent use. [Manager.NewListener],
// [Manager.GetListenerByID], [Manager.Notify], and [Listener.Cancel] may all be called from
// multiple goroutines simultaneously.
package notify
