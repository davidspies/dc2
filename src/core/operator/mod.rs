mod concat;
mod dynamic;
mod input;
mod join;
mod map;
mod reduce;
mod split;

pub use self::dynamic::{Collection, DynOp, DynReceiver};
pub use self::input::Input;
pub use self::split::Receiver;
use super::Step;
use crate::core::is_map::IsAddMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use std::collections::HashMap;

pub trait Operator {
    type D: Key;
    type R: Monoid;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R>;
}

fn default_flow_to<C: Op>(this: &mut C, step: Step) -> HashMap<C::D, C::R> {
    let mut res = HashMap::new();
    this.flow(step, |x, r| res.add(x, r));
    res
}

pub trait Op: Operator {
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: Step, send: F);
}
