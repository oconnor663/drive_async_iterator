#![feature(async_iterator)]
#![feature(async_for_loop)]
#![feature(gen_blocks)]
#![feature(yield_expr)]
#![allow(unused_features)]

use drive_async_iterator::{NeverDone, drive};
use futures::future::{BoxFuture, FutureExt, pending, ready};
use futures::stream::FuturesUnordered;
use std::async_iter::{AsyncIterator, PollNext};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::{Mutex, oneshot};
use tokio::time::{Duration, sleep, timeout};

#[tokio::test]
async fn test_drive() {
    let mut finished = false;
    drive!(iter = futures::stream::iter([1, 2, 3]), {
        assert_eq!(iter.next().await, Some(1));
        assert_eq!(iter.next().await, Some(2));
        assert_eq!(iter.next().await, Some(3));
        assert_eq!(iter.next().await, None);
        finished = true;
    },); // Test that the trailing comma is allowed.
    assert!(finished);
}

#[tokio::test]
async fn test_mutex() {
    async fn foo() {
        static LOCK: Mutex<()> = Mutex::const_new(());
        let _guard = LOCK.lock().await;
        sleep(Duration::from_millis(10)).await;
    }
    let futures = FuturesUnordered::new();
    futures.push(foo());
    futures.push(foo());
    drive!(futures, {
        while let Some(_) = futures.next().await {
            foo().await; // Should not deadlock here!
        }
    },); // Test that the trailing comma is allowed.
}

#[tokio::test]
async fn test_delayed_next() {
    let lock = Mutex::new(());
    let futures = FuturesUnordered::new();
    let guard = lock.lock().await;
    futures.push(async {
        drop(guard);
        42
    });
    assert!(lock.try_lock().is_err());
    drive!(futures, {
        // `futures` should make progress even before the call to `next`, so `guard` should get
        // dropped promptly. If not, we'll deadlock here.
        let _guard = lock.lock().await;
        assert_eq!(futures.next().await, Some(42));
        assert_eq!(futures.next().await, None);
    });
}

#[tokio::test]
async fn test_cancelled_next() {
    let futures = FuturesUnordered::new();
    futures.push(async {
        sleep(Duration::from_millis(10)).await;
        42
    });
    let mut iterations = 0;
    drive!(futures, {
        // Call `next` in a loop with a tight timeout. It'll get cancelled several times before it
        // eventually returns the item.
        loop {
            iterations += 1;
            let one_ms = Duration::from_millis(1);
            if let Ok(Some(x)) = timeout(one_ms, futures.next()).await {
                assert_eq!(x, 42);
                break;
            }
        }
    });
    assert!(iterations > 5);
}

#[tokio::test]
async fn test_poll_progress_after_item() {
    struct ProgressAfterItem {
        item_yielded: bool,
        sender: Option<oneshot::Sender<()>>,
    }
    impl AsyncIterator for ProgressAfterItem {
        type Item = ();
        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> PollNext<()> {
            if self.item_yielded {
                PollNext::Pending
            } else {
                self.item_yielded = true;
                PollNext::Item(())
            }
        }
        fn poll_progress(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            if self.item_yielded
                && let Some(sender) = self.sender.take()
            {
                _ = sender.send(());
            }
            Poll::Ready(())
        }
    }
    let (sender, mut receiver) = oneshot::channel();
    let iter = ProgressAfterItem {
        item_yielded: false,
        sender: Some(sender),
    };
    drive!(iter, {
        assert_eq!(iter.next().await, Some(()));
        // At this point we've taken an item from the stream, but `poll_progress` hasn't yet been
        // called. (It better for performance if we don't call it every time.)
        assert!(receiver.try_recv().is_err());
        // However, now we're going to go wait on something else, and we need to make sure
        // `poll_progress` gets called "on the way out" so that in general it can register wakeups.
        // If it doesn't, the channel setup in this test will deadlock.
        receiver.await.unwrap();
    });
}

#[tokio::test]
async fn test_concurrent_nexts() {
    drive!(iter = futures::stream::iter([1, 2]), {
        let (option1, option2) = futures::future::join(iter.next(), iter.next()).await;
        let mut items = [option1.unwrap(), option2.unwrap()];
        items.sort();
        assert_eq!(items, [1, 2]);
    });
}

#[tokio::test]
async fn test_pending_then_done() {
    // This async generator returns `Pending` at first but soon reports `Done` without ever
    // yielding an item. Test that that doesn't confuse the state machine.
    async gen fn foo() {
        sleep(Duration::from_millis(10)).await;
    }
    drive!(iter = foo(), {
        assert_eq!(iter.next().await, None);
    });
}

#[tokio::test]
async fn test_with_mut() {
    use futures::FutureExt;
    drive!(iter = FuturesUnordered::new(), {
        iter.with_mut(|maybe_iter| {
            maybe_iter.unwrap().push(
                async {
                    sleep(Duration::from_millis(10)).await;
                    42
                }
                .boxed(),
            );
        });
        assert_eq!(iter.next().await, Some(42));
        // Test `with_pin_mut` also, which doesn't require `Unpin`.
        iter.with_pin_mut(|maybe_iter| {
            maybe_iter.unwrap().push(
                async {
                    sleep(Duration::from_millis(10)).await;
                    99
                }
                .boxed(),
            );
        });
        assert_eq!(iter.next().await, Some(99));
        assert_eq!(iter.next().await, None);
    });
}

#[tokio::test]
async fn test_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    let future = async {
        drive!(iter = futures::stream::iter([1, 2, 3]), {
            while let Some(_) = iter.next().await {}
        });
    };
    assert_send_sync(&future);
}

#[tokio::test]
async fn test_with_mut_after_drop() {
    drive!(iter = futures::stream::iter([()]), {
        iter.with_mut(|maybe| assert!(maybe.is_some()));
        assert!(iter.next().await.is_some());
        iter.with_mut(|maybe| assert!(maybe.is_some()));
        assert!(iter.next().await.is_none());
        iter.with_mut(|maybe| assert!(maybe.is_none()));
        assert!(iter.next().await.is_none());
        iter.with_mut(|maybe| assert!(maybe.is_none()));
    });
}

#[tokio::test]
#[should_panic]
async fn test_reentrant_with_mut() {
    drive!(iter = futures::stream::iter([()]), {
        iter.with_mut(|_| {
            iter.with_pin_mut(|_| {});
        });
    });
}

#[tokio::test]
#[should_panic]
async fn test_reentrant_with_mut_the_other_way() {
    // Same as above, but reversing inner and outer just for fun.
    drive!(iter = futures::stream::iter([()]), {
        iter.with_pin_mut(|_| {
            iter.with_mut(|_| {});
        });
    });
}

#[tokio::test]
async fn test_never_done_add_work_after_cancelled_next() {
    async fn forty_two() -> u32 {
        42
    }
    let mut futures = NeverDone::new(FuturesUnordered::new());
    futures.as_mut().push(forty_two());
    drive!(futures, {
        assert_eq!(futures.next().await, Some(42));
        // At this point the `FuturesUnordered` is empty. `next` would block forever, because the
        // `FuturesUnordered` is returning `Done`, and `NeverDone` converts that to `Pending`.
        let one_ms = Duration::from_millis(1);
        assert!(timeout(one_ms, futures.next()).await.is_err());
        // Now, add more work and call `next` again. We're testing that this doesn't deadlock.
        futures.with_mut(|f| f.unwrap().as_mut().push(forty_two()));
        assert_eq!(futures.next().await, Some(42));
    });
}

#[tokio::test]
async fn test_bare_add_work_after_cancelled_next() {
    // This is like the test above, except it doesn't use `NeverDone`. Instead, we start the
    // `FuturesUnordered` off with a `pending` future that will never complete, so that it can
    // never return `Done`. This is an example of why we need to handle mutation-related wakeups in
    // `with_mut`, rather than relying on `NeverDone` to trigger wakeups for us.
    let futures: FuturesUnordered<BoxFuture<()>> = FuturesUnordered::new();
    futures.push(pending().boxed());
    drive!(futures, {
        // Start a call to `next` but cancel it after a short while. This also absorbs any
        // immediate wakeups that `FuturesUnordered` might trigger. See:
        // https://github.com/rust-lang/futures-rs/blob/f68806c1205a6495d5c381bc9180d162e791b010/futures-util/src/stream/futures_unordered/mod.rs#L545
        assert!(timeout(Duration::from_millis(10), futures.next()).await.is_err());
        // Now add another future that will be ready immediately.
        futures.with_mut(|maybe_futures| {
            maybe_futures.unwrap().push(ready(()).boxed());
        });
        // Wait on that future. This shouldn't deadlock!
        assert_eq!(futures.next().await, Some(()));
    });
}
