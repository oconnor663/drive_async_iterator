With the introduction of `poll_progress`, the `.next()` method on `Stream`/`AsyncIterator` is
probably going away. The most common use case can be replaced with a `for await` loop, but more
complicated use cases are hard to translate. This macro wraps `for await` and defines a
`next().await` function within its body. It runs the `for await` concurrently with its body, so
the deadlock-free behavior of `poll_progress` is preserved.

This is experimental code.
