use super::{DynOperator, Operator, Receiver};
use crate::key::Key;
use crate::monoid::Monoid;
use crate::CWrapper;
use crate::Step;
use std::collections::HashMap;

pub type TCollection<D, R> = Receiver<WCollection<D, R>>;
pub type Collection<D, R> = CWrapper<TCollection<D, R>>;

pub struct WCollection<D, R>(Box<dyn DynOperator<D = D, R = R>>);

impl<D: Key, R: Monoid> DynOperator for WCollection<D, R> {
    type D = D;
    type R = R;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R> {
        self.0.flow_to(step)
    }
}

impl<D: Key, R: Monoid> Operator for WCollection<D, R> {
    fn flow<F: FnMut(D, R)>(&mut self, step: Step, mut send: F) {
        let res = self.0.flow_to(step);
        for (x, r) in res {
            send(x, r)
        }
    }
}

impl<C: Operator> CWrapper<C> {
    pub fn wcollect(self) -> CWrapper<WCollection<C::D, C::R>>
    where
        C: 'static,
    {
        CWrapper {
            inner: WCollection(Box::new(self.inner)),
            context_id: self.context_id,
        }
    }
    pub fn collect(self) -> Collection<C::D, C::R>
    where
        C: 'static,
    {
        self.wcollect().split()
    }
}
