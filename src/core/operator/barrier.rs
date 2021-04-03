use super::Op;
use crate::core::node::Node;
use crate::core::{Relation, Step};

pub struct Barrier<C> {
    pub(super) inner: Node<C>,
    step: usize,
}

impl<C: Op> Barrier<C> {
    pub(super) fn new(inner: Node<C>) -> Self {
        Barrier { inner, step: 0 }
    }
    pub(super) fn dirty(&self, step: Step) -> bool {
        self.step < step
    }
}

impl<C: Op> Op for Barrier<C> {
    type D = C::D;
    type R = C::R;
    fn default_op_name() -> &'static str {
        "barrier"
    }
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: Step, send: F) {
        if self.inner.needs_update(self.step, step) {
            self.step = step;
            self.inner.flow(step, send);
        }
    }
}

impl<C: Op> Relation<C> {
    /// Checks to see if there have been any calls to `commit` since the last time the underlying
    /// relation was read
    /// before proceeding to propagate changes from the input. Note that this function is already
    /// called by `self.split()`. In general, the user should not need to call this
    /// explicitly (however there is an alias for this function: `relation.enter()` which should
    /// generally be used on inputs to subgraphs).
    pub fn barrier(self) -> Relation<Barrier<C>> {
        Relation::new(vec![self.dep()], Barrier::new(self.inner)).hidden()
    }
}
