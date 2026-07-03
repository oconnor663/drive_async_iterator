## `drive_async_iterator`

> **HEADS UP:** This experimental macro currently requires a [fork of
> rustc](https://github.com/oconnor663/rust/pull/2) to build, because it depends on the
> proposed `AsyncIterator::poll_progress` method.

With the introduction of `AsyncIterator::poll_progress`, the `.next()` method on
`Stream`/`StreamExt` is probably not going to work, because there's no way for it to fulfill
the `poll_progress` contract. The most common use case of `.next()` can be replaced with a `for
await` loop, but more complicated use cases are hard to translate. This macro recreates
`.next()` in a `poll_progress`-compatible form, by taking ownership of an `AsyncIterator` and
providing a handle with a `.next()` method. It calls `poll_progress` concurrently with its body
when an `.await` other than `.next().await` is pending, so the new `AsyncIterator` contract is
satisfied.

A caller that previously used `Stream`/`StreamExt` this way...

```rust
let mut my_stream = pin!(stream::iter([1, 2, 3]));
assert_eq!(my_stream.next().await, Some(1));
assert_eq!(my_stream.next().await, Some(2));
assert_eq!(my_stream.next().await, Some(3));
assert_eq!(my_stream.next().await, None);
```

...can use `AsyncIterator` + `poll_progress` this way (note that `pin!` is gone):

```rust
use drive_async_iterator::drive;

drive!(my_stream = async_iter::from_iter([1, 2, 3]), {
    assert_eq!(my_stream.next().await, Some(1));
    assert_eq!(my_stream.next().await, Some(2));
    assert_eq!(my_stream.next().await, Some(3));
    assert_eq!(my_stream.next().await, None);
});
```

There are two ways to invoke the macro. As above, there's `drive!(<name> = <iter>, <body>)`.
And for cases where you would write `<name> = <name>`, there's the shorthand `drive!(<name>, <body>)`.

### Fixing deadlocks

Besides helping with migration, this macro solves a [class of
deadlocks](https://jacko.io/snooze.html) that present-day `.next()` loops are vulnerable to. Or
rather, `poll_progress` solves them, and this macro wires `.next()` into `poll_progress`.
Consider this example ([playground link][playground_deadlock]):

[playground_deadlock]: <https://play.rust-lang.org/?version=stable&mode=debug&edition=2024&gist=88087e5b73a1697d62e743966dfe3f10>

```rust
// This function acquires a static `Mutex` and does a brief sleep,
// simulating some sort of IO with a shared resource.
async fn foo() {
    static LOCK: Mutex<()> = Mutex::const_new(());
    let _guard = LOCK.lock().await;
    sleep(Duration::from_millis(10)).await;
}

let mut futures = FuturesUnordered::new();
futures.push(foo());
futures.push(foo());
while let Some(_) = futures.next().await {
    foo().await; // Deadlock!
}
```

That example deadlocks because one of the `foo` futures in the [`FuturesUnordered`] is holding
the `LOCK`, but it's not making progress. Using `drive!` the same loop runs smoothly, because
`FuturesUnordered` can poll its contents concurrently with the loop body:

```rust
let mut futures = FuturesUnordered::new();
futures.push(foo());
futures.push(foo());
drive!(futures, {
    while let Some(_) = futures.next().await {
        foo().await; // Not a deadlock!
    }
});
```

### More complicated cases

The example above could work with a standard `for await` loop, so it doesn't necessarily need
the `drive!` macro. However, one of the powerful features of [`FuturesUnordered`] (and also for
example [`StreamMap`]) is that you can add more work to it while it's running ([playground
link][playground_select]):

[playground_select]: https://play.rust-lang.org/?version=stable&mode=debug&edition=2024&gist=46fd466a8a7893a54cf576df84334ef6

```rust
let mut futures = FuturesUnordered::new();
loop {
    select! {
        Some(_) = futures.next() => {
            // Do something with the result...
        }
        job = more_work() => futures.push(job),
    }
}
```

That example isn't going to work with `for await`, for two reasons:

1. We need access to `futures` in the loop body, but `for await` takes ownership of it.
2. When `FuturesUnordered` is empty, its `poll_next` method returns `Done`. But that makes `for
   await` and also the `drive!` macro drop it immediately. We need it to return `Pending`
   instead.

For the first problem, the handle provided by `drive!` supports the `with_pin_mut` and (for
`Unpin` types) `with_mut` methods. For the second problem, this crate provides a
`NeverDone` async iterator adapter. Putting those two things together, we can implement the
example above while still calling `poll_progress` correctly under the hood:

```rust
drive!(futures = NeverDone::new(FuturesUnordered::new()), {
    loop {
        select! {
            Some(_) = futures.next() => {
                // Do something with the result...
            }
            job = more_work() => {
                futures.with_mut(|maybe_futures: Option<_>| {
                    let futures = maybe_futures.expect("never dropped");
                    futures.push(job);
                });
            }
        }
    }
});
```

Note that calling `next` on a `NeverDone` async iterator will never return `None`. Instead,
it'll block potentially forever waiting for the iterator to yield more items. That's different
from the behavior of `StreamExt::next` today, which returns `None` immediately in the empty
case. The blocking behavior avoids accidental hot loops in some cases, but it can also cause
some existing callers to block forever unexpectedly.

[`FuturesUnordered`]: https://docs.rs/futures/latest/futures/stream/struct.FuturesUnordered.html
[`StreamMap`]: https://docs.rs/tokio-stream/latest/tokio_stream/struct.StreamMap.html
[`Waker`]: https://doc.rust-lang.org/core/task/struct.Waker.html
