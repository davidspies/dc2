use super::{default_flow_to, DynOperator, Operator};
use crate::key::Key;
use crate::monoid::Monoid;
use crate::{Relation, Step};
use std::collections::HashMap;

struct Concat<C1, C2> {
    left: C1,
    right: C2,
}

impl<D: Key, R: Monoid, C1: Operator<D = D, R = R>, C2: Operator<D = D, R = R>> DynOperator
    for Concat<C1, C2>
{
    type D = D;
    type R = R;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}

impl<D: Key, R: Monoid, C1: Operator<D = D, R = R>, C2: Operator<D = D, R = R>> Operator
    for Concat<C1, C2>
{
    fn flow<F: FnMut(D, R)>(&mut self, step: Step, mut send: F) {
        self.left.flow(step, &mut send);
        self.right.flow(step, send);
    }
}

impl<C: Operator> Relation<C> {
    pub fn concat<C2: Operator<D = C::D, R = C::R>>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Operator<D = C::D, R = C::R>> {
        assert_eq!(self.context_id, other.context_id, "Context mismatch");
        Relation {
            inner: Concat {
                left: self.inner,
                right: other.inner,
            },
            context_id: self.context_id,
        }
    }
}
