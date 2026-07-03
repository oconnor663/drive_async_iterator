//! With the introduction of `poll_progress`, the `.next()` method on `Stream`/`AsyncIterator` is
//! probably going away. The most common use case can be replaced with a `for await` loop, but more
//! complicated use cases are hard to translate. This macro aims to make it easier to migrate
//! callers who use `.next()` in nontrivial ways. It takes ownership of an `AsyncIterator` and
//! wraps it in a type with a `.next()` method. It also calls `poll_progress` concurrently with the
//! body when an `.await` other than `next().await` is pending, following the new `AsyncIterator`
//! contract.
//!
//! Besides easing migration, this macro solves a [class of
//! deadlocks](https://jacko.io/snooze.html) that present-day `.next()` loops are vulnerable to.
//! (Or rather, `poll_progress` solves these deadlocks, and this macro calls it correctly.)
//! Consider this example ([playground link]):
//!
//! [playground link]: <https://play.rust-lang.org/?version=stable&mode=debug&edition=2024&gist=88087e5b73a1697d62e743966dfe3f10>
//!
//! ```no_run
//! use futures::StreamExt;
//! use futures::stream::FuturesUnordered;
//! use std::time::Duration;
//! use tokio::sync::Mutex;
//!
//! async fn foo() {
//!     static LOCK: Mutex<()> = Mutex::const_new(());
//!     let _guard = LOCK.lock().await;
//!     tokio::time::sleep(Duration::from_millis(10)).await;
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut futures = FuturesUnordered::new();
//!     futures.push(foo());
//!     futures.push(foo());
//!     while let Some(_) = futures.next().await {
//!         foo().await; // Deadlock!
//!     }
//! }
//! ```
//!
//! That example deadlocks because one of the `foo` futures in the `FuturesUnordered` is holding
//! the `LOCK`, but it's not making progress. Updating that example to use `drive!` fixes the
//! deadlock:
//!
//! ```
//! # #![feature(async_iterator)]
//! # use futures::StreamExt;
//! # use futures::stream::FuturesUnordered;
//! # use std::time::Duration;
//! # use tokio::sync::Mutex;
//! # async fn foo() {
//! #     static LOCK: Mutex<()> = Mutex::const_new(());
//! #     let _guard = LOCK.lock().await;
//! #     tokio::time::sleep(Duration::from_millis(10)).await;
//! # }
//! # #[tokio::main]
//! # async fn main() {
//! use drive_async_iterator::drive;
//!
//! let mut futures = FuturesUnordered::new();
//! futures.push(foo());
//! futures.push(foo());
//! drive!(futures, {
//!     while let Some(_) = futures.next().await {
//!         foo().await; // Not a deadlock!
//!     }
//! });
//! # }
//! ```
//!
//! This experimental macro currently requires a [fork of
//! rustc](https://github.com/oconnor663/rust/pull/2) to build, because it depends on the proposed
//! `AsyncIterator::poll_progress` method.

#![no_std]
#![feature(async_iterator)]

use atomic_refcell::AtomicRefCell;
use core::async_iter::{AsyncIterator, PollNext};

#[macro_export]
macro_rules! drive {
    ($driven_iter:ident, $body:expr $(,)?) => {
        $crate::drive!($driven_iter = $driven_iter, $body)
    };
    ($driven_iter:ident = $iter:expr, $body:expr $(,)?) => {{
        // SAFETY: This struct must not be moved after this point.
        let state = unsafe {
            $crate::_impl::AtomicRefCell::new(
                $crate::_impl::DriveState::new(
                    $iter
                )
            )
        };
        let $driven_iter = $crate::_impl::new_driven_async_iterator(&state);
        let mut poll_next_future = ::core::pin::pin!(async {
            loop {
                let mut state = state.borrow_mut();
                // If `next` is cancelled, `item` might not get take immediately. In that case it
                // might already be `Some`.
                if state.next_item_wanted && state.item.is_none() {
                    // `DriveState` handles fusing and dropping `iterator` internally.
                    if let ::core::async_iter::PollNext::Item(item) = state.poll_next_once().await {
                        state.item = Some(item);
                        state.next_item_wanted = false;
                        // Now we're handing an item off to the body. We need to call
                        // `poll_progress` before this whole macro yields, but we'd rather not call
                        // it now if the body is going to ask for another item immediately.
                        // Instead, we let the outer loop do it right before it finally yields.
                    }
                }
                // Don't want to keep the `state` borrowed across this yield.
                drop(state);
                $crate::_impl::pending_once().await;
            }
        });
        let mut body_future = ::core::pin::pin!(async {
            $body
        });
        loop {
            // Polling the poll-next-future is a no-op once the iterator is done.
            _ = $crate::_impl::poll_once(poll_next_future.as_mut()).await;
            let body_poll = $crate::_impl::poll_once(body_future.as_mut()).await;
            if let ::core::task::Poll::Ready(output) = body_poll {
                break output;
            }
            let mut state = state.borrow_mut();
            if !state.next_item_wanted {
                // The body is awaiting something other than `next`, possibly after some calls
                // to `next` have yielded items. This is where we call `poll_progress`, so that
                // in general we only call it once after a chain of ready items.
                _ = state.poll_progress_once().await;
            }
            // Either the iterator side is awaiting the next item, or the body side is awaiting
            // something else, or both. They will wake us up.
            //
            // As above, don't keep `state` borrowed across this yield.
            drop(state);
            $crate::_impl::pending_once().await;
        }
    }};
}

pub struct DrivenAsyncIterator<'a, Iter: AsyncIterator> {
    state: &'a AtomicRefCell<_impl::DriveState<Iter>>,
}

impl<Iter: AsyncIterator> DrivenAsyncIterator<'_, Iter> {
    pub async fn next(&self) -> Option<Iter::Item> {
        loop {
            let mut state = self.state.borrow_mut();
            if let Some(item) = state.item.take() {
                return Some(item);
            }
            if state.iterator_is_done() {
                return None;
            }
            // NOTE: `next_item_wanted` might already be true if there are concurrent calls to
            // `next, or if a previous call was cancelled. That's fine. If a concurrent call beats
            // us to the item, it'll clear `next_item_wanted`, and then we'll restore it.
            if !state.next_item_wanted {
                // There's no buffered item, and the iterator isn't already in a `poll_next` loop.
                // Try calling `poll_next` ourselves. This is a fake await (just for `Context`),
                // which is guaranteed to be ready immediately.
                match state.poll_next_once().await {
                    // If we get an item, we can just return it. That'll leave `next_item_wanted`
                    // false, and the outer loop will do a `poll_progress` for us.
                    PollNext::Item(item) => return Some(item),
                    // `DriveState` handles fusing and dropping `iterator` internally.
                    PollNext::Done => return None,
                    PollNext::Pending => {}
                }
                // `poll_next` has returned `Pending`, and the `AsyncIterator` contract says we
                // need to keep calling `poll_next` until it yields an item. However, it's not
                // enough to do that within this function, because this function could be
                // cancelled. (If this function *owned* the iterator, then cancelling it would drop
                // the iterator, and the contract would be satisfied.) We need to set
                // `next_item_wanted` and trust the poll-next-future to take care of this for us.
                state.next_item_wanted = true;
            }
            // Yield without arranging our own wakeup, trusting the poll-next-future to make
            // progress for us. Don't keep `state` borrowed across this.
            drop(state);
            _impl::pending_once().await;
        }
    }
}

/// Functions that are only intended for use by the macro
#[doc(hidden)]
pub mod _impl {
    use core::async_iter::{AsyncIterator, PollNext};
    use core::pin::Pin;
    use core::task::{Context, Poll};

    // The macro needs `AtomicRefCell` internally.
    pub use atomic_refcell::AtomicRefCell;

    pub fn new_driven_async_iterator<'a, Iter: AsyncIterator>(
        state: &'a AtomicRefCell<DriveState<Iter>>,
    ) -> super::DrivenAsyncIterator<'a, Iter> {
        super::DrivenAsyncIterator { state }
    }

    pub struct DriveState<Iter: AsyncIterator> {
        iterator: Option<Iter>, // unsafe pinned
        pub item: Option<Iter::Item>,
        pub next_item_wanted: bool,
    }

    impl<Iter: AsyncIterator> DriveState<Iter> {
        // This struct pins `iterator`, but taking a dependency on e.g. `pin_project_lite` would
        // add build time and make this complicated macro even more complicated. We obviously never
        // move this after constructing it, and we never expose our instance to the caller either
        // (it gets a hygienic name), so we could almost get away with leaving this API technically
        // unsound. But for macro reasons it has to be public, so we make this constructor unsafe
        // to establish soundness with only a small change to the macroexpanded code.
        pub unsafe fn new(iter: Iter) -> Self {
            Self {
                iterator: Some(iter),
                item: None,
                next_item_wanted: false,
            }
        }

        fn iterator_pinned(&mut self) -> Option<Pin<&mut Iter>> {
            if let Some(iter) = &mut self.iterator {
                // SAFETY: `new` is unsafe, and this field is private.
                Some(unsafe { Pin::new_unchecked(iter) })
            } else {
                None
            }
        }

        pub fn iterator_is_done(&self) -> bool {
            self.iterator.is_none()
        }

        /// This drops `iterator` the first time it returns `Done`, and it keeps returning `Done`
        /// after that. (In other words, the iterator is effectively "fused".)
        pub async fn poll_next_once(&mut self) -> PollNext<Iter::Item> {
            pub struct PollNextOnce<'a, Iter>(Pin<&'a mut Iter>);
            impl<Iter: AsyncIterator> Future for PollNextOnce<'_, Iter> {
                type Output = PollNext<Iter::Item>;
                fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                    Poll::Ready(self.0.as_mut().poll_next(cx))
                }
            }
            if let Some(iter) = self.iterator_pinned() {
                let poll_next = PollNextOnce(iter).await;
                if let PollNext::Done = poll_next {
                    self.iterator = None;
                }
                poll_next
            } else {
                PollNext::Done
            }
        }

        /// If `iterator` has returned `Done` and been dropped, then this returns `Ready(())`.
        pub async fn poll_progress_once(&mut self) -> Poll<()> {
            pub struct PollProgressOnce<'a, Iter>(Pin<&'a mut Iter>);
            impl<Iter: AsyncIterator> Future for PollProgressOnce<'_, Iter> {
                type Output = Poll<()>;
                fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                    Poll::Ready(self.0.as_mut().poll_progress(cx))
                }
            }
            if let Some(iter) = self.iterator_pinned() {
                PollProgressOnce(iter).await
            } else {
                Poll::Ready(())
            }
        }
    }

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
}
