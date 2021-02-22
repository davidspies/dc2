use super::Op;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::Relation;
use crate::core::Step;
use std::marker::PhantomData;

pub struct DynOp<D, R = isize>(Box<dyn DynOpT<D = D, R = R>>);

trait DynOpT: 'static {
    type D: Key;
    type R: Monoid;
    fn flow_dyn(&mut self, step: &Step, send: &mut dyn FnMut(Self::D, Self::R));
}

impl<T: Op> DynOpT for T {
    type D = <T as Op>::D;
    type R = <T as Op>::R;
    fn flow_dyn(&mut self, step: &Step, send: &mut dyn FnMut(Self::D, Self::R)) {
        self.flow(step, send)
    }
}

impl<D: Key, R: Monoid> Op for DynOp<D, R> {
    type D = D;
    type R = R;
    fn flow<F: FnMut(D, R)>(&mut self, step: &Step, mut send: F) {
        self.0.flow_dyn(step, &mut send)
    }
}

impl<'a, C: Op> Relation<'a, C> {
    pub fn dynamic(self) -> Relation<'a, DynOp<C::D, C::R>> {
        Relation {
            inner: DynOp(Box::new(self.inner)),
            context_id: self.context_id,
            depth: self.depth,
            phantom: PhantomData,
        }
    }
}
