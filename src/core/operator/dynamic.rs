use super::Op;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::Relation;
use crate::core::Step;

pub struct DynOp<D, R>(Box<dyn DynOpT<D = D, R = R>>);

trait DynOpT {
    type D: Key;
    type R: Monoid;
    fn flow_dyn<'a>(&mut self, step: Step, send: Box<dyn FnMut(Self::D, Self::R) + 'a>);
}

impl<T: Op> DynOpT for T {
    type D = <T as Op>::D;
    type R = <T as Op>::R;
    fn flow_dyn<'a>(&mut self, step: Step, send: Box<dyn FnMut(Self::D, Self::R) + 'a>) {
        self.flow(step, send)
    }
}

impl<D: Key, R: Monoid> Op for DynOp<D, R> {
    type D = D;
    type R = R;
    fn flow<F: FnMut(D, R)>(&mut self, step: Step, send: F) {
        self.0.flow_dyn(step, Box::new(send))
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
