use super::Op;
use crate::core::Relation;
use std::ops::AddAssign;

struct Map<C, MF> {
    _inner: C,
    _op: MF,
}

impl<D1, R1, D2: 'static, R2: AddAssign<R2>, C: Op<D = D1, R = R1>, MF: Fn(D1, R1) -> (D2, R2)> Op
    for Map<C, MF>
{
    type D = D2;
    type R = R2;
}

impl<C: Op> Relation<C> {
    pub fn map_dr<F: Fn(C::D, C::R) -> (D2, R2), D2: 'static, R2: AddAssign<R2>>(
        self,
        f: F,
    ) -> Relation<impl Op<D = D2, R = R2>> {
        Relation {
            inner: Map {
                _inner: self.inner,
                _op: f,
            },
        }
    }
}
