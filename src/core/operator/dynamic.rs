use super::{Op, Operator, Receiver};
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::Relation;
use crate::core::Step;
use std::collections::HashMap;

pub type DynReceiver<D, R> = Receiver<DynOp<D, R>>;
pub type Collection<D, R> = Relation<DynReceiver<D, R>>;

pub struct DynOp<D, R>(Box<dyn Operator<D = D, R = R>>);

impl<D: Key, R: Monoid> Operator for DynOp<D, R> {
    type D = D;
    type R = R;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R> {
        self.0.flow_to(step)
    }
}

impl<D: Key, R: Monoid> Op for DynOp<D, R> {
    fn flow<F: FnMut(D, R)>(&mut self, step: Step, mut send: F) {
        let res = self.0.flow_to(step);
        for (x, r) in res {
            send(x, r)
        }
    }
}

impl<C: Op> Relation<C> {
    pub fn dynamic(self) -> Relation<DynOp<C::D, C::R>>
    where
        C: 'static,
    {
        Relation {
            inner: DynOp(Box::new(self.inner)),
            context_id: self.context_id,
        }
    }
    pub fn collect(self) -> Collection<C::D, C::R>
    where
        C: 'static,
    {
        self.dynamic().split()
    }
}
