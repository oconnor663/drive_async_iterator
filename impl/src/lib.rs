use proc_macro2::TokenStream as TokenStream2;
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
        tokens.extend(quote! {
            println!("hello world");
        });
    }
}

#[proc_macro]
pub fn drive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let c = parse_macro_input!(input as Drive);
    quote! { #c }.into()
}
