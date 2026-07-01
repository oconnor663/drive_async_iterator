//! With the introduction of `poll_progress`, the `.next()` method on `Stream`/`AsyncIterator` is
//! probably going away. The most common use case can be replaced with a `for await` loop, but more
//! complicated use cases are hard to translate. This macro wraps `for await` and defines a
//! `next().await` function within its body. It runs the `for await` concurrently with its body, so
//! the deadlock-free behavior of `poll_progress` is preserved.
//!
//! This is experimental code.

#![no_std]

use atomic_refcell::AtomicRefCell;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};

/// The macro that this crate is all about
///
/// See the [module-level documentation](crate) for details and examples.
pub use join_me_maybe_impl::join;

/// The type that provides the `.cancel()` method for labeled arguments
pub struct Canceller<'a> {
    cancelled: &'a AtomicBool,
    // The `cancelled_count` is only needed for a very niche purpose: to detect when *dropping* a
    // cancelled future/stream cancels a different one. See the test case
    // `test_drop_during_cancellation_can_cancel_other_arms`.
    cancelled_count: &'a AtomicUsize,
    finished: Option<&'a AtomicBool>, // only Some for "definitely" arms
    definitely_count: &'a AtomicUsize,
}

impl<'a> Canceller<'a> {
    /// Cancel the corresponding labeled future or stream. It won't be polled again, and it'll be
    /// dropped promptly by the `join!` (though not directly within this method).
    pub fn cancel(&self) {
        // We can't drop the corresponding future/stream here, even if we had a reference to it,
        // because in the self-cancellation case the AtomicRefCell would panic.
        let already_cancelled = self.cancelled.swap(true, Relaxed);
        if !already_cancelled {
            // The cancelled count is needed for a *very* specific purpose
            self.cancelled_count.fetch_add(1, Relaxed);
        }
        if let Some(finished) = self.finished {
            let already_finished = finished.swap(true, Relaxed);
            if !already_finished {
                self.definitely_count.fetch_add(1, Relaxed);
            }
        }
    }
}

impl<'a> core::fmt::Debug for Canceller<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Canceller").finish_non_exhaustive()
    }
}

/// The canceller type visible in arm bodies, which supports [`with_pin_mut`][Self::with_pin_mut]
/// and [`with_mut`][Self::with_mut].
pub struct CancellerMut<'a, T> {
    canceller: Canceller<'a>,
    labeled_cell: &'a AtomicRefCell<Pin<&'a mut Option<T>>>,
}

impl<'a, T> CancellerMut<'a, T> {
    /// Cancel the corresponding labeled future or stream. It won't be polled again, and it'll be
    /// dropped promptly by the `join!` (though not directly within this method).
    pub fn cancel(&self) {
        self.canceller.cancel();
    }

    /// Obtain a short-lived `Pin<&mut T>` pointing to the labeled future or stream, for the
    /// duration of the provided closure. If the labeled arm has already finished or been
    /// cancelled, the closure receives `None` instead but still runs.
    ///
    /// Internally, each labeled arm is owned by an [`AtomicRefCell`]. If you nest calls to
    /// `with_pin_mut` and try to borrow the same arm twice, the second call will panic.
    ///
    /// [`AtomicRefCell`]: https://docs.rs/atomic_refcell/latest/atomic_refcell/struct.AtomicRefCell.html
    pub fn with_pin_mut<F, U>(&self, f: F) -> U
    where
        F: FnOnce(Option<Pin<&mut T>>) -> U,
    {
        f(self.labeled_cell.borrow_mut().as_mut().as_pin_mut())
    }

    /// Like [`with_pin_mut`][Self::with_pin_mut] above but without `Pin`. This requires the
    /// underlying type to be `Unpin`.
    pub fn with_mut<F, U>(&self, f: F) -> U
    where
        F: FnOnce(Option<&mut T>) -> U,
        T: Unpin,
    {
        f(self.labeled_cell.borrow_mut().as_mut().get_mut().as_mut())
    }
}

// SAFETY: CancellerMut is Send+Sync whenever T is Send, for the same reason as std::sync::Mutex.
// It only hands out mutable references to its contents. (I.e. there is no `with_pin_ref` method.)
// If the contents are !Sync, those references won't be able to escape the thread they wind up on
// for as long as they're alive.
unsafe impl<'a, T: Send> Send for CancellerMut<'a, T> {}
unsafe impl<'a, T: Send> Sync for CancellerMut<'a, T> {}

impl<'a, T> core::fmt::Debug for CancellerMut<'a, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CancellerMut").finish_non_exhaustive()
    }
}

/// Functions that are only intended for use by the macro
#[doc(hidden)]
pub mod _impl {
    use super::*;
    use core::task::{Context, Poll};
    use futures::{FutureExt, Stream, StreamExt};

    pub fn new_canceller<'a>(
        cancelled: &'a AtomicBool,
        cancelled_count: &'a AtomicUsize,
        finished: Option<&'a AtomicBool>,
        definitely_count: &'a AtomicUsize,
    ) -> Canceller<'a> {
        Canceller {
            cancelled,
            cancelled_count,
            finished,
            definitely_count,
        }
    }

    pub fn new_canceller_mut<'a, T>(
        cancelled: &'a AtomicBool,
        cancelled_count: &'a AtomicUsize,
        finished: Option<&'a AtomicBool>,
        definitely_count: &'a AtomicUsize,
        labeled_cell: &'a AtomicRefCell<Pin<&'a mut Option<T>>>,
    ) -> CancellerMut<'a, T> {
        CancellerMut {
            canceller: Canceller {
                cancelled,
                cancelled_count,
                finished,
                definitely_count,
            },
            labeled_cell,
        }
    }

    // `futures` has `poll!`, but it doesn't have a stream version. The macro is also kind of
    // gross, so just adapt it into a couple wrapper structs
    pub struct PollOnce<Fut: Future + Unpin>(pub Fut);

    impl<Fut: Future + Unpin> Future for PollOnce<Fut> {
        type Output = Poll<Fut::Output>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(self.0.poll_unpin(cx))
        }
    }

    pub struct PollNextOnce<S: Stream + Unpin>(pub S);

    impl<S: Stream + Unpin> Future for PollNextOnce<S> {
        type Output = Poll<Option<S::Item>>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(self.0.poll_next_unpin(cx))
        }
    }

    // The macro needs `AtomicRefCell` internally.
    pub use atomic_refcell::AtomicRefCell;

    // This is the only thing from `futures` that the macro needs internally. It's also annoying to
    // deal with post-`cargo expand`. Just wrap it.
    pub async fn yield_once() {
        futures::pending!();
    }
}
