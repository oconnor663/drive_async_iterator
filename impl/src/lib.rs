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
        let for_await_proceed = format_ident!("for_await_proceed", span = Span::mixed_site());
        let for_await_done = format_ident!("for_await_done", span = Span::mixed_site());
        let outer_loop_again = format_ident!("outer_loop_again", span = Span::mixed_site());
        let item = format_ident!("item", span = Span::mixed_site());
        tokens.extend(quote! {{
            let #for_await_proceed = ::core::sync::atomic::AtomicBool::new(false);
            let #for_await_done = ::core::sync::atomic::AtomicBool::new(false);
            let #outer_loop_again = ::core::sync::atomic::AtomicBool::new(false);
            let #item = ::drive_async_iterator::_impl::AtomicRefCell::new(None);
            let mut for_await_future = ::core::pin::pin!(async {
                while !#for_await_proceed.load(::core::sync::atomic::Ordering::Relaxed) {
                    ::drive_async_iterator::_impl::pending_once().await;
                }
                // TODO: NOT CORRECT! WE NEED TO poll_progress BEFORE THE LOOP
                // (also we want mutable access)
                for await x in #iterator {
                    let mut item_mut = #item.borrow_mut();
                    debug_assert!(item_mut.is_none());
                    *item_mut = Some(x);
                    drop(item_mut);
                    #for_await_proceed.store(false, ::core::sync::atomic::Ordering::Relaxed);
                    while !#for_await_proceed.load(::core::sync::atomic::Ordering::Relaxed) {
                        ::drive_async_iterator::_impl::pending_once().await;
                    }
                }
                #for_await_done.store(true, ::core::sync::atomic::Ordering::Relaxed);
            });
            let mut body_future = ::core::pin::pin!(async {
                // Intentionally non-hygienic!
                let next = async || {
                    #for_await_proceed.store(true, ::core::sync::atomic::Ordering::Relaxed);
                    #outer_loop_again.store(true, ::core::sync::atomic::Ordering::Relaxed);
                    loop {
                        if let Some(item) = #item.borrow_mut().take() {
                            return Some(item);
                        }
                        if #for_await_done.load(::core::sync::atomic::Ordering::Relaxed) {
                            return None;
                        }
                        // Yield without arranging a wakeup! The loop side has arranged one, and
                        // the outer loop will poll it before it polls us again.
                        ::drive_async_iterator::_impl::pending_once().await;
                    }
                };
                #body
            });
            loop {
                if !#for_await_done.load(::core::sync::atomic::Ordering::Relaxed) {
                    _ = ::drive_async_iterator::_impl::poll_once(for_await_future.as_mut()).await;
                };
                let body_poll = ::drive_async_iterator::_impl::poll_once(body_future.as_mut()).await;
                if let ::core::task::Poll::Ready(output) = body_poll {
                    break output;
                }
                if !#outer_loop_again.swap(false, ::core::sync::atomic::Ordering::Relaxed) {
                    ::drive_async_iterator::_impl::pending_once().await;
                }
            }
        }});
    }
}

#[proc_macro]
pub fn drive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let c = parse_macro_input!(input as Drive);
    quote! { #c }.into()
}
