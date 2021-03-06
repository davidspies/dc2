#![feature(map_first_last)]
#![feature(type_alias_impl_trait)]

mod convenience_operators;
mod core;
pub mod map;
mod tuple;

pub use self::convenience_operators::{
    Collection, DynReceiver, MapMapArrangement, MappingArrangement, OrderedArrangement,
};
pub use self::core::{
    borrow, emptyable, key, monoid, subgraph, Arrangement, CreationContext, DynOp,
    ExecutionContext, Input, IsReduce, Op, Receiver, ReduceOutput, Relation,
};

#[cfg(test)]
mod tests;
