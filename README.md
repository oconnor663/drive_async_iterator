With the introduction of `poll_progress`, the `.next()` method on `Stream`/`AsyncIterator` is
probably going away. The most common use case can be replaced with a `for await` loop, but more
complicated use cases are hard to translate. This macro aims to make it easier to migrate
callers who use `.next()` in nontrivial ways. It takes ownership of an `AsyncIterator` and
wraps it in a type with a `.next()` method. It also calls `poll_progress` concurrently with the
body when an `.await` other than `next().await` is pending, following the new `AsyncIterator`
contract.

Besides easing migration, this macro solves a [class of
deadlocks](https://jacko.io/snooze.html) that present-day `.next()` loops are vulnerable to.
(Or rather, `poll_progress` solves these deadlocks, and this macro calls it correctly.)
Consider this example ([playground link]):

[playground link]: <https://play.rust-lang.org/?version=stable&mode=debug&edition=2024&gist=88087e5b73a1697d62e743966dfe3f10>

```rust
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::time::Duration;
use tokio::sync::Mutex;

async fn foo() {
    static LOCK: Mutex<()> = Mutex::const_new(());
    let _guard = LOCK.lock().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
}

#[tokio::main]
async fn main() {
    let mut futures = FuturesUnordered::new();
    futures.push(foo());
    futures.push(foo());
    while let Some(_) = futures.next().await {
        foo().await; // Deadlock!
    }
}
```

That example deadlocks because one of the `foo` futures in the `FuturesUnordered` is holding
the `LOCK`, but it's not making progress. Updating that example to use `drive!` fixes the
deadlock:

```rust
use drive_async_iterator::drive;

let mut futures = FuturesUnordered::new();
futures.push(foo());
futures.push(foo());
drive!(futures, {
    while let Some(_) = futures.next().await {
        foo().await; // Not a deadlock!
    }
});
```

This experimental macro currently requires a [fork of
rustc](https://github.com/oconnor663/rust/pull/2) to build, because it depends on the proposed
`AsyncIterator::poll_progress` method.
