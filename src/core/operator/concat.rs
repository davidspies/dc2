use super::Op;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::{Relation, Step};
use std::marker::PhantomData;

struct Concat<C1, C2> {
    left: C1,
    right: C2,
}

impl<D: Key, R: Monoid, C1: Op<D = D, R = R>, C2: Op<D = D, R = R>> Op for Concat<C1, C2> {
    type D = D;
    type R = R;

    fn flow<F: FnMut(D, R)>(&mut self, step: Step, mut send: F) {
        self.left.flow(step, &mut send);
        self.right.flow(step, send);
    }
}

impl<'a, C: Op> Relation<'a, C> {
    pub fn concat<C2: Op<D = C::D, R = C::R>>(
        self,
        other: Relation<C2>,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>> {
        assert_eq!(self.context_id, other.context_id, "Context mismatch");
        Relation {
            inner: Concat {
                left: self.inner,
                right: other.inner,
            },
            context_id: self.context_id,
            depth: self.depth.max(other.depth),
            phantom: PhantomData,
        }
    }
}
