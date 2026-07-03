//! # `drive_async_iterator`
//!
//! With the introduction of `AsyncIterator::poll_progress`, the `.next()` method on
//! `Stream`/`StreamExt` is probably not going to work, because there's no way for it to fulfill
//! the `poll_progress` contract. The most common use case of `.next()` can be replaced with a `for
//! await` loop, but more complicated use cases are hard to translate. This macro recreates
//! `.next()` in a `poll_progress`-compatible form, by taking ownership of an `AsyncIterator` and
//! providing a handle with a `.next()` method. It calls `poll_progress` concurrently with its body
//! when an `.await` other than `.next().await` is pending, so the new `AsyncIterator` contract is
//! satisfied.
//!
//! A caller that previously used `Stream`/`StreamExt` this way...
//!
//! ```
//! # use futures::stream::{self, StreamExt};
//! # use std::pin::pin;
//! # #[tokio::main]
//! # async fn main() {
//! let mut my_stream = pin!(stream::iter([1, 2, 3]));
//! assert_eq!(my_stream.next().await, Some(1));
//! assert_eq!(my_stream.next().await, Some(2));
//! assert_eq!(my_stream.next().await, Some(3));
//! assert_eq!(my_stream.next().await, None);
//! # }
//! ```
//!
//! ...can use `AsyncIterator` + `poll_progress` this way (note that `pin!` is gone):
//!
//! ```
//! # #![feature(async_iterator)]
//! # #![feature(async_iter_from_iter)]
//! # use std::async_iter;
//! # #[tokio::main]
//! # async fn main() {
//! use drive_async_iterator::drive;
//!
//! drive!(my_stream = async_iter::from_iter([1, 2, 3]), {
//!     assert_eq!(my_stream.next().await, Some(1));
//!     assert_eq!(my_stream.next().await, Some(2));
//!     assert_eq!(my_stream.next().await, Some(3));
//!     assert_eq!(my_stream.next().await, None);
//! });
//! # }
//! ```
//!
//! There are two ways to invoke the macro. As above, there's `drive!(<name> = <iter>, <body>)`.
//! And for cases where you would write `<name> = <name>`, there's the shorthand `drive!(<name>, <body>)`.
//!
//! Besides helping with migration, this macro solves a [class of
//! deadlocks](https://jacko.io/snooze.html) that present-day `.next()` loops are vulnerable to. Or
//! rather, `poll_progress` solves them, and this macro wires `.next()` into `poll_progress`.
//! Consider this example ([playground link]):
//!
//! [playground link]: <https://play.rust-lang.org/?version=stable&mode=debug&edition=2024&gist=88087e5b73a1697d62e743966dfe3f10>
//!
//! ```no_run
//! # use futures::StreamExt;
//! # use futures::stream::FuturesUnordered;
//! # use tokio::sync::Mutex;
//! # use tokio::time::{Duration, sleep};
//! #
//! // This function acquires a static `Mutex` and does a brief sleep,
//! // simulating some sort of IO with a shared resource.
//! async fn foo() {
//!     static LOCK: Mutex<()> = Mutex::const_new(());
//!     let _guard = LOCK.lock().await;
//!     sleep(Duration::from_millis(10)).await;
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let mut futures = FuturesUnordered::new();
//! futures.push(foo());
//! futures.push(foo());
//! while let Some(_) = futures.next().await {
//!     foo().await; // Deadlock!
//! }
//! # }
//! ```
//!
//! That example deadlocks because one of the `foo` futures in the `FuturesUnordered` is holding
//! the `LOCK`, but it's not making progress. But with `drive!`, the same loop runs smoothly,
//! because `FuturesUnordered` drives its contents concurrently with the loop:
//!
//! ```
//! # #![feature(async_iterator)]
//! # use drive_async_iterator::drive;
//! # use futures::StreamExt;
//! # use futures::stream::FuturesUnordered;
//! # use tokio::sync::Mutex;
//! # use tokio::time::{Duration, sleep};
//! #
//! # // This function acquires a static `Mutex` and does a brief sleep,
//! # // simulating some sort of IO with a shared resource.
//! # async fn foo() {
//! #     static LOCK: Mutex<()> = Mutex::const_new(());
//! #     let _guard = LOCK.lock().await;
//! #     sleep(Duration::from_millis(10)).await;
//! # }
//! #
//! # #[tokio::main]
//! # async fn main() {
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
use core::pin::Pin;

/// Take ownership of an `AsyncIterator` and provide a handle with an async [`.next()`] method.
///
/// [`.next()`]: DrivenAsyncIterator::next
#[macro_export]
macro_rules! drive {
    ($driven_iter:ident, $body:expr $(,)?) => {
        $crate::drive!($driven_iter = $driven_iter, $body)
    };
    ($driven_iter:ident = $iter:expr, $body:expr $(,)?) => {{
        let state_pin = ::core::pin::pin!($crate::_impl::DriveState::new($iter));
        let state_cell = $crate::_impl::AtomicRefCell::new(state_pin);
        let $driven_iter = $crate::_impl::new_driven_async_iterator(&state_cell);
        let mut poll_next_future = ::core::pin::pin!(async {
            loop {
                let mut state = state_cell.borrow_mut();
                // If `next` is cancelled, `item` might not get take immediately. In that case it
                // might already be `Some`.
                if *state.as_mut().next_item_wanted() && state.as_mut().item().is_none() {
                    // `DriveState` handles fusing and dropping `iterator` internally.
                    if let ::core::async_iter::PollNext::Item(item) =
                        state.as_mut().poll_next_once().await
                    {
                        *state.as_mut().item() = Some(item);
                        *state.as_mut().next_item_wanted() = false;
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
        let mut body_future = ::core::pin::pin!(async { $body });
        loop {
            // Polling the poll-next-future is a no-op once the iterator is done.
            _ = $crate::_impl::poll_once(poll_next_future.as_mut()).await;
            let body_poll = $crate::_impl::poll_once(body_future.as_mut()).await;
            if let ::core::task::Poll::Ready(output) = body_poll {
                break output;
            }
            let mut state = state_cell.borrow_mut();
            if !*state.as_mut().next_item_wanted() {
                // The body is awaiting something other than `next`, possibly after some calls
                // to `next` have yielded items. This is where we call `poll_progress`, so that
                // in general we only call it once after a chain of ready items.
                _ = state.as_mut().poll_progress_once().await;
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

/// The `AsyncIterator` wrapper type that `drive!` provides to its body
pub struct DrivenAsyncIterator<'a, 'b, Iter: AsyncIterator> {
    state: &'a AtomicRefCell<Pin<&'b mut _impl::DriveState<Iter>>>,
}

impl<Iter: AsyncIterator> DrivenAsyncIterator<'_, '_, Iter> {
    /// Get the next item from the async iterator.
    ///
    /// Note that this method takes `&self`, and multiple futures are allowed to call it
    /// concurrently. However, if you manage to call `next` from multiple _threads_, it's likely to
    /// panic.
    ///
    /// Implementation details: `DrivenAsyncIterator` uses an [`AtomicRefCell`] internally, rather
    /// than a [`Mutex`]. This makes it more efficient and also `no_std`-compatible. The downside
    /// is panics when it's accessed from parallel threads, but this should be extremely unlikely
    /// in practice. `DrivenAsyncIterator` has a local lifetime bound, so it can't be given to
    /// [`task::spawn`] or [`thread::spawn`]. Scoped tasks also [don't currently exist][trilemma].
    /// To trigger this panic, you'd have to use something like [`thread::scope`] in an async
    /// context, which would be unusual and potentially an executor-blocking bug in any case.
    ///
    /// [`AtomicRefCell`]: https://docs.rs/atomic_refcell/latest/atomic_refcell/struct.AtomicRefCell.html
    /// [`Mutex`]: https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html
    /// [`task::spawn`]: https://docs.rs/tokio/latest/tokio/task/fn.spawn.html
    /// [`thread::spawn`]: https://doc.rust-lang.org/std/thread/fn.spawn.html
    /// [`thread::scope`]: https://doc.rust-lang.org/std/thread/fn.scope.html
    /// [trilemma]: https://without.boats/blog/the-scoped-task-trilemma/
    pub async fn next(&self) -> Option<Iter::Item> {
        loop {
            let mut state = self.state.borrow_mut();
            if let Some(item) = state.as_mut().item().take() {
                return Some(item);
            }
            if state.as_mut().iterator().is_none() {
                return None;
            }
            // NOTE: `next_item_wanted` might already be true if there are concurrent calls to
            // `next, or if a previous call was cancelled. That's fine. If a concurrent call beats
            // us to the item, it'll clear `next_item_wanted`, and then we'll restore it.
            if !*state.as_mut().next_item_wanted() {
                // There's no buffered item, and the iterator isn't already in a `poll_next` loop.
                // Try calling `poll_next` ourselves. This is a fake await (just for `Context`),
                // which is guaranteed to be ready immediately.
                match state.as_mut().poll_next_once().await {
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
                *state.as_mut().next_item_wanted() = true;
            }
            // Yield without arranging our own wakeup, trusting the poll-next-future to make
            // progress for us. Don't keep `state` borrowed across this.
            drop(state);
            _impl::pending_once().await;
        }
    }

    pub fn with_pin_mut<F, Ret>(&self, f: F) -> Ret
    where
        F: FnOnce(Option<Pin<&mut Iter>>) -> Ret,
    {
        let mut state = self.state.borrow_mut();
        f(state.as_mut().iterator().as_pin_mut())
    }

    pub fn with_mut<F, Ret>(&self, f: F) -> Ret
    where
        F: FnOnce(Option<&mut Iter>) -> Ret,
        Iter: Unpin,
    {
        let mut state = self.state.borrow_mut();
        f(state.as_mut().iterator().get_mut().as_mut())
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

    pub fn new_driven_async_iterator<'a, 'b, Iter: AsyncIterator>(
        state: &'a AtomicRefCell<Pin<&'b mut DriveState<Iter>>>,
    ) -> super::DrivenAsyncIterator<'a, 'b, Iter> {
        super::DrivenAsyncIterator { state }
    }

    pin_project_lite::pin_project! {
        pub struct DriveState<Iter: AsyncIterator> {
            #[pin]
            iterator: Option<Iter>,
            item: Option<Iter::Item>,
            next_item_wanted: bool,
        }
    }

    impl<Iter: AsyncIterator> DriveState<Iter> {
        pub fn new(iter: Iter) -> Self {
            Self {
                iterator: Some(iter),
                item: None,
                next_item_wanted: false,
            }
        }

        pub fn iterator(self: Pin<&mut Self>) -> Pin<&mut Option<Iter>> {
            self.project().iterator
        }

        pub fn item(self: Pin<&mut Self>) -> &mut Option<Iter::Item> {
            self.project().item
        }

        pub fn next_item_wanted(self: Pin<&mut Self>) -> &mut bool {
            self.project().next_item_wanted
        }

        /// This drops `iterator` the first time it returns `Done`, and it keeps returning `Done`
        /// after that. (In other words, the iterator is effectively "fused".)
        pub async fn poll_next_once(mut self: Pin<&mut Self>) -> PollNext<Iter::Item> {
            pub struct PollNextOnce<'a, Iter>(Pin<&'a mut Iter>);
            impl<Iter: AsyncIterator> Future for PollNextOnce<'_, Iter> {
                type Output = PollNext<Iter::Item>;
                fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                    Poll::Ready(self.0.as_mut().poll_next(cx))
                }
            }
            if let Some(iter) = self.as_mut().iterator().as_pin_mut() {
                let poll_next = PollNextOnce(iter).await;
                if let PollNext::Done = poll_next {
                    self.as_mut().iterator().set(None);
                }
                poll_next
            } else {
                PollNext::Done
            }
        }

        /// If `iterator` has returned `Done` and been dropped, then this returns `Ready(())`.
        pub async fn poll_progress_once(self: Pin<&mut Self>) -> Poll<()> {
            pub struct PollProgressOnce<'a, Iter>(Pin<&'a mut Iter>);
            impl<Iter: AsyncIterator> Future for PollProgressOnce<'_, Iter> {
                type Output = Poll<()>;
                fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                    Poll::Ready(self.0.as_mut().poll_progress(cx))
                }
            }
            if let Some(iter) = self.iterator().as_pin_mut() {
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
