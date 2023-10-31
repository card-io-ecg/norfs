#![cfg_attr(not(test), no_std)]
#![feature(async_fn_in_trait)]
#![feature(impl_trait_projections)]
#![feature(generic_const_exprs)] // Eww
#![allow(incomplete_features, stable_features, async_fn_in_trait, unknown_lints)]

pub mod aligned;
pub mod medium;
