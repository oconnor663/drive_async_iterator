#![feature(async_iterator)]
#![feature(async_for_loop)]
#![feature(gen_blocks)]
#![feature(yield_expr)]
#![allow(unused_features)]

use drive_async_iterator::drive;
use futures::stream::FuturesUnordered;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep, timeout};

#[tokio::test]
async fn test_drive() {
    let mut finished = false;
    drive!(futures::stream::iter([1, 2, 3]), {
        assert_eq!(next().await, Some(1));
        assert_eq!(next().await, Some(2));
        assert_eq!(next().await, Some(3));
        assert_eq!(next().await, None);
        finished = true;
    });
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
        while let Some(_) = next().await {
            foo().await; // Should not deadlock here!
        }
    });
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
        assert_eq!(next().await, Some(42));
        assert_eq!(next().await, None);
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
            if let Ok(Some(x)) = timeout(one_ms, next()).await {
                assert_eq!(x, 42);
                break;
            }
        }
    });
    assert!(iterations > 5);
}

#[tokio::test]
async fn test_concurrent_nexts() {
    drive!(futures::stream::iter([1, 2]), {
        let (option1, option2) = futures::future::join(next(), next()).await;
        let mut items = [option1.unwrap(), option2.unwrap()];
        items.sort();
        assert_eq!(items, [1, 2]);
    });
}
