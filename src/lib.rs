//! With the introduction of `poll_progress`, the `.next()` method on `Stream`/`AsyncIterator` is
//! probably going away. The most common use case can be replaced with a `for await` loop, but more
//! complicated use cases are hard to translate. This macro aims to make it easier to migrate
//! callers who use `.next()` in nontrivial ways. It takes ownership of an `AsyncIterator` and
//! defines a `next().await` function within its body. It calls `poll_progress` concurrently with
//! the body when an `.await` other than `next().await` is pending, following the new
//! `AsyncIterator` contract.
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
//!     while let Some(_) = next().await {
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

    pub struct DriveState<Iter: AsyncIterator> {
        iterator: Option<Iter>, // unsafe pinned
        pub item: Option<Iter::Item>,
        pub next_item_wanted: bool,
        pub outer_loop_again: bool,
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
                outer_loop_again: false,
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

        pub fn iterator_done(&self) -> bool {
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
