use crate::{Op, Relation};
use std::ops::AddAssign;
use std::ops::Mul;

impl<C: Op> Relation<C> {
    pub fn map<F: Fn(C::D) -> D2 + 'static, D2: 'static>(
        self,
        f: F,
    ) -> Relation<impl Op<D = D2, R = C::R>> {
        self.map_dr(move |x, r| (f(x), r))
    }
}

impl<K: 'static, V: 'static, C: Op<D = (K, V)>> Relation<C> {
    pub fn semijoin<C2: Op<D = K, R = R2>, R2, R3: AddAssign<R3>>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<R2, Output = R3>,
    {
        self.join(other.map(|x| (x, ()))).map(|(k, x, ())| (k, x))
    }
}
