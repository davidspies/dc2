use super::{Op, Receiver};
use crate::core::is_map::IsAddMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::Relation;
use crate::core::Step;
use std::collections::HashMap;

pub type DynReceiver<D, R> = Receiver<DynOp<D, R>>;

pub struct DynOp<D, R>(Box<dyn DOp<D = D, R = R>>);

trait DOp {
    type D: Key;
    type R: Monoid;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R>;
}

impl<T: Op> DOp for T {
    type D = <T as Op>::D;
    type R = <T as Op>::R;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R> {
        let mut res = HashMap::new();
        self.flow(step, |x, r| res.add(x, r));
        res
    }
}

impl<D: Key, R: Monoid> Op for DynOp<D, R> {
    type D = D;
    type R = R;
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
}
