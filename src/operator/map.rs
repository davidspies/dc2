use super::{default_flow_to, DynOperator, Operator};
use crate::key::Key;
use crate::monoid::Monoid;
use crate::{Relation, Step};
use std::collections::HashMap;

struct FlatMap<C, MF> {
    inner: C,
    op: MF,
}

impl<
        D1,
        R1,
        D2: Key,
        R2: Monoid,
        C: Operator<D = D1, R = R1>,
        I: IntoIterator<Item = (D2, R2)>,
        MF: Fn(D1, R1) -> I,
    > DynOperator for FlatMap<C, MF>
{
    type D = D2;
    type R = R2;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}

impl<
        D1,
        R1,
        D2: Key,
        R2: Monoid,
        C: Operator<D = D1, R = R1>,
        I: IntoIterator<Item = (D2, R2)>,
        MF: Fn(D1, R1) -> I,
    > Operator for FlatMap<C, MF>
{
    fn flow<F: FnMut(D2, R2)>(&mut self, step: Step, mut send: F) {
        let FlatMap {
            ref mut inner,
            ref op,
        } = self;
        inner.flow(step, |x, r| {
            for (x2, r2) in op(x, r) {
                send(x2, r2)
            }
        })
    }
}

impl<C: Operator> Relation<C> {
    pub fn flat_map_r<
        F: Fn(C::D, C::R) -> I,
        D2: Key,
        R2: Monoid,
        I: IntoIterator<Item = (D2, R2)>,
    >(
        self,
        f: F,
    ) -> Relation<impl Operator<D = D2, R = R2>> {
        Relation {
            inner: FlatMap {
                inner: self.inner,
                op: f,
            },
            context_id: self.context_id,
        }
    }
}
