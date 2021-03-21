use super::Op;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::node::Node;
use crate::core::{Relation, Step};

struct FlatMap<C, MF> {
    inner: Node<C>,
    op: MF,
}

impl<
        D1,
        R1,
        D2: Key,
        R2: Monoid,
        C: Op<D = D1, R = R1>,
        I: IntoIterator<Item = (D2, R2)>,
        MF: Fn(D1, R1) -> I + 'static,
    > Op for FlatMap<C, MF>
{
    type D = D2;
    type R = R2;

    fn default_op_name() -> &'static str {
        "flat_map_dr"
    }
    fn flow<F: FnMut(D2, R2)>(&mut self, step: &Step, mut send: F) {
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

impl<'a, C: Op> Relation<'a, C> {
    pub fn flat_map_dr<
        F: Fn(C::D, C::R) -> I + 'static,
        D2: Key,
        R2: Monoid,
        I: IntoIterator<Item = (D2, R2)>,
    >(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = D2, R = R2>> {
        Relation::new(
            vec![self.dep()],
            FlatMap {
                inner: self.inner,
                op: f,
            },
        )
    }
}
