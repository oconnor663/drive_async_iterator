//! With the introduction of `poll_progress`, the `.next()` method on `Stream`/`AsyncIterator` is
//! probably going away. The most common use case can be replaced with a `for await` loop, but more
//! complicated use cases are hard to translate. This macro wraps `AsyncIterator` and defines a
//! `next().await` function within its body. It calls `poll_progress` concurrently with the body,
//! so the `AsyncIterator` contract is respected regardless of whether or when `next` is called. In
//! particular, touching the same lock in the iterator and in the body should never deadlock,
//! unless the body calls `next` *while holding that lock*.
//!
//! This is experimental code.

#![no_std]
#![feature(async_iterator)]

/// The macro that this crate is all about
///
/// See the [module-level documentation](crate) for details and examples.
pub use drive_async_iterator_impl::drive;

/// Functions that are only intended for use by the macro
#[doc(hidden)]
pub mod _impl {
    use core::async_iter::{AsyncIterator, PollNext};
    use core::pin::Pin;
    use core::task::{Context, Poll};

    // The macro needs `AtomicRefCell` internally.
    pub use atomic_refcell::AtomicRefCell;

    #[inline]
    pub fn pending_once() -> PendingOnce {
        PendingOnce { yielded: false }
    }

    pub struct PendingOnce {
        yielded: bool,
    }

    impl Future for PendingOnce {
        type Output = ();

        #[inline]
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                Poll::Pending
            }
        }
    }

    #[inline]
    pub fn poll_once<Fut>(fut: Pin<&mut Fut>) -> PollOnce<'_, Fut> {
        PollOnce(fut)
    }

    pub struct PollOnce<'a, Fut>(Pin<&'a mut Fut>);

    impl<Fut: Future> Future for PollOnce<'_, Fut> {
        type Output = Poll<Fut::Output>;

        #[inline]
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(self.0.as_mut().poll(cx))
        }
    }

    #[inline]
    pub fn poll_next_once<Iter>(iter: Pin<&mut Iter>) -> PollNextOnce<'_, Iter> {
        PollNextOnce(iter)
    }

    pub struct PollNextOnce<'a, Iter>(Pin<&'a mut Iter>);

    impl<Iter: AsyncIterator> Future for PollNextOnce<'_, Iter> {
        type Output = PollNext<Iter::Item>;

        #[inline]
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(self.0.as_mut().poll_next(cx))
        }
    }

    #[inline]
    pub fn poll_progress_once<Iter>(iter: Pin<&mut Iter>) -> PollProgressOnce<'_, Iter> {
        PollProgressOnce(iter)
    }

    pub struct PollProgressOnce<'a, Iter>(Pin<&'a mut Iter>);

    impl<Iter: AsyncIterator> Future for PollProgressOnce<'_, Iter> {
        type Output = Poll<()>;

        #[inline]
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(self.0.as_mut().poll_progress(cx))
        }
    }
}
