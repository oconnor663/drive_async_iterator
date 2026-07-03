## `drive_async_iterator`

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

Besides helping with migration, this macro solves a [class of
deadlocks](https://jacko.io/snooze.html) that present-day `.next()` loops are vulnerable to. Or
rather, `poll_progress` solves them, and this macro wires `.next()` into `poll_progress`.
Consider this example ([playground link]):

[playground link]: <https://play.rust-lang.org/?version=stable&mode=debug&edition=2024&gist=88087e5b73a1697d62e743966dfe3f10>

```rust
#
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

That example deadlocks because one of the `foo` futures in the `FuturesUnordered` is holding
the `LOCK`, but it's not making progress. But with `drive!`, the same loop runs smoothly,
because `FuturesUnordered` drives its contents concurrently with the loop:

```rust
#
#
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
