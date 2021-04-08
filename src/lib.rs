#![feature(coerce_unsized)]
#![feature(map_first_last)]
#![feature(type_alias_impl_trait)]

mod convenience_operators;
mod core;
pub mod feedback;
pub mod map;
mod tuple;

pub use self::convenience_operators::{
    Collection, DynReceiver, MapMapArrangement, MappingArrangement, OrderedArrangement,
};
pub use self::core::{
    borrow,
    context::{CreationContext, ExecutionContext},
    emptyable, key, monoid, Arrangement, ArrangementG, DynOp, Input, IsReduce, Op, Receiver,
    ReduceOutput, Relation,
};

#[cfg(test)]
mod tests;
