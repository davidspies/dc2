mod barrier;
mod concat;
mod consolidate;
mod dynamic;
mod input;
mod join;
mod map;
mod reduce;
mod split;
pub mod subgraph;
mod triangles;

pub use self::dynamic::DynOp;
pub use self::input::Input;
pub use self::reduce::{IsReduce, ReduceOutput};
pub use self::split::Receiver;
use super::Step;
use crate::core::key::Key;
use crate::core::monoid::Monoid;

pub trait Op: 'static {
    type D: Key;
    type R: Monoid;
    fn hideable() -> bool {
        true
    }
    fn default_op_name() -> &'static str;
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: &Step, send: F);
}
