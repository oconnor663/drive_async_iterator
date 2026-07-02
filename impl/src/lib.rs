use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Expr, parse_macro_input};

struct Drive {
    iterator: Expr,
    body: Expr,
}

impl Parse for Drive {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let iterator = input.parse()?;
        let _ = input.parse::<syn::Token![,]>()?;
        let body = input.parse()?;
        if input.peek(syn::Token![,]) {
            let _ = input.parse::<syn::Token![,]>()?;
        }
        if !input.is_empty() {
            return Err(input.error("unexpected trailing tokens after drive body"));
        }
        Ok(Drive { iterator, body })
    }
}

impl ToTokens for Drive {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        let Self { iterator, body } = self;
        let state = format_ident!("state", span = Span::mixed_site());
        let iterator_future = format_ident!("iterator_future", span = Span::mixed_site());
        let body_future = format_ident!("body_future", span = Span::mixed_site());
        tokens.extend(quote! {{
            // SAFETY: This struct must not be moved after this point.
            let #state = unsafe {
                ::drive_async_iterator::_impl::AtomicRefCell::new(
                    ::drive_async_iterator::_impl::DriveState::new(
                        #iterator
                    )
                )
            };
            let mut #iterator_future = ::core::pin::pin!(async {
                loop {
                    let mut state = #state.borrow_mut();
                    // NOTE: If `next` is cancelled, `item` might not get take immediately. In that
                    // case it might already be `Some`.
                    if state.next_item_wanted && state.item.is_none() {
                        debug_assert!(state.item.is_none());
                        // NOTE: `DriveState` handles fusing and dropping `iterator` internally.
                        if let ::core::async_iter::PollNext::Item(item) = state.poll_next_once().await {
                            state.item = Some(item);
                            state.next_item_wanted = false;
                        }
                    } else {
                        _ = state.poll_progress_once().await;
                    }
                    // `poll_next` or `poll_progress` above might have register a wakeup. If not,
                    // we'll rely entirely on wakeups from the body side, and on the fact that
                    // `next` sets `outer_loop_again`.
                    //
                    // The awaits above are "fake" in that they're guaranteed to be ready
                    // immediately, but this one will actually yield control, so we don't want to
                    // hold the `state` borrow across it.
                    drop(state);
                    ::drive_async_iterator::_impl::pending_once().await;
                }
            });
            let mut #body_future = ::core::pin::pin!(async {
                // Intentionally non-hygienic!
                let next = async || {
                    let mut first_iteration = true;
                    loop {
                        // NOTE: Even though we could call `poll_next` directly through the
                        // `AtomicRefCell` here, we *shouldn't*, because this async function can be
                        // cancelled. Once `poll_next` is called, we need to guarantee that we'll
                        // keep calling it and not switch back to `poll_progress` before the next
                        // item is ready. That's why we rely entirely on the `next_item_wanted`
                        // state flag, rather than polling the iterator directly here.
                        let mut state = #state.borrow_mut();
                        if let Some(item) = state.item.take() {
                            return Some(item);
                        }
                        if state.iterator_done() {
                            return None;
                        }
                        // NOTE: Even though we could call `poll_next` directly through the
                        // `AtomicRefCell` here, we *shouldn't*, because this async function can be
                        // cancelled. Once `poll_next` is called, we need to guarantee that we'll
                        // keep calling it and not switch back to `poll_progress` before the next
                        // item is ready. That's why we rely entirely on the `next_item_wanted`
                        // state flag, rather than polling the iterator directly here.
                        //
                        // Also, `next_item_wanted` might already be true if a previous call to
                        // `next` was cancelled. That's fine.
                        state.next_item_wanted = true;
                        // We poll the iterator future before the body future. When `next` is first
                        // called, we need to re-run the outer loop to give the body future a
                        // chance to call `poll_next`. If it doesn't give us an item immediately,
                        // it'll handle its own wakeups after that.
                        if first_iteration {
                            first_iteration = false;
                            state.outer_loop_again = true;
                        }
                        // Yield without arranging our own wakeup, trusting the iterator future to
                        // make progress for us.
                        //
                        // The awaits above are "fake" in that they're guaranteed to be ready
                        // immediately, but this one will actually yield control, so we don't want
                        // to hold the `state` borrow across it.
                        drop(state);
                        ::drive_async_iterator::_impl::pending_once().await;
                    }
                };
                #body
            });
            loop {
                // Polling the iterator future is a no-op once the iterator is done.
                _ = ::drive_async_iterator::_impl::poll_once(#iterator_future.as_mut()).await;
                let body_poll = ::drive_async_iterator::_impl::poll_once(#body_future.as_mut()).await;
                if let ::core::task::Poll::Ready(output) = body_poll {
                    break output;
                }
                let mut state = #state.borrow_mut();
                if state.outer_loop_again {
                    state.outer_loop_again = false;
                    continue;
                }
                // Either the iterator side is awaiting the next item, or the body side is awaiting
                // something else, or both. They will wake us up.
                //
                // The awaits above are "fake" in that they're guaranteed to be ready immediately,
                // but this one will actually yield control, so we don't want to hold the `state`
                // borrow across it.
                drop(state);
                ::drive_async_iterator::_impl::pending_once().await;
            }
        }});
    }
}

#[proc_macro]
pub fn drive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let c = parse_macro_input!(input as Drive);
    quote! { #c }.into()
}
