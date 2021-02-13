#![feature(map_first_last)]

mod convenience_operators;
mod core;
pub mod map;

pub use self::convenience_operators::Collection;
pub use self::core::{
    borrow, emptyable, key, monoid, Arrangement, CreationContext, DynOp, DynReceiver,
    ExecutionContext, Input, Op, Receiver, Relation,
};

#[cfg(test)]
mod tests;
