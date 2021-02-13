mod collect;
mod concat;
mod input;
mod join;
mod map;
mod reduce;
mod split;

pub use self::collect::{Collection, TCollection, WCollection};
pub use self::input::{Input, InputCollection};
pub use self::split::Receiver;
use super::Step;
use crate::is_map::IsAddMap;
use crate::key::Key;
use crate::monoid::Monoid;
use std::collections::HashMap;

pub trait DynOperator {
    type D: Key;
    type R: Monoid;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R>;
}

fn default_flow_to<C: Operator>(this: &mut C, step: Step) -> HashMap<C::D, C::R> {
    let mut res = HashMap::new();
    this.flow(step, |x, r| res.add(x, r));
    res
}

pub trait Operator: DynOperator {
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: Step, send: F);
}
