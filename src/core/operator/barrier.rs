use super::Op;
use crate::core::{Relation, Step};
use std::marker::PhantomData;

pub struct Barrier<C> {
    pub(super) inner: C,
    depth: usize,
    step: usize,
}

impl<C> Barrier<C> {
    pub(super) fn new(inner: C, depth: usize) -> Self {
        Barrier {
            inner,
            depth,
            step: 0,
        }
    }
    pub(super) fn dirty(&self, step: &Step) -> bool {
        let step_for_depth = step.step_for(self.depth);
        let against = step_for_depth.get_last();
        self.step < against
    }
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
    /// Checks to see if there have been any calls to `commit` since the last time the underlying
    /// relation was read
    /// before proceeding to propagate changes from the input. Note that this function is already
    /// called by `self.split()`. In general, the user should not need to call this
    /// explicitly (however there is an alias for this function: `relation.enter()` which should
    /// generally be used on inputs to subgraphs).
    pub fn barrier(self) -> Relation<'a, Barrier<C>> {
        Relation {
            inner: Barrier::new(self.inner, self.depth),
            context_id: self.context_id,
            depth: self.depth,
            phantom: PhantomData,
        }
    }
}
