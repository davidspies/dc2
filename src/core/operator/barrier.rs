use super::Op;
use crate::core::{Relation, Step};
use std::marker::PhantomData;

pub struct Barrier<C> {
    inner: C,
    depth: usize,
    step: usize,
}

impl<C: Op> Op for Barrier<C> {
    type D = C::D;
    type R = C::R;
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: &Step, send: F) {
        let step_for_depth = step.step_for(self.depth);
        let against = step_for_depth.get_last();
        if self.step < against {
            self.step = against;
            self.inner.flow(step_for_depth, send);
        }
    }
}

impl<'a, C: Op> Relation<'a, C> {
    pub fn barrier(self) -> Relation<'a, Barrier<C>> {
        Relation {
            inner: Barrier {
                inner: self.inner,
                depth: self.depth,
                step: 0,
            },
            context_id: self.context_id,
            depth: self.depth,
            phantom: PhantomData,
        }
    }
}
