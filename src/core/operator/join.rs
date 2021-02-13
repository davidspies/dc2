use super::Op;
use crate::core::Relation;
use std::ops::{AddAssign, Mul};

struct Join<LC, RC> {
    _left: LC,
    _right: RC,
}

impl<
        LC: Op<D = (K, LD), R = LR>,
        RC: Op<D = (K, RD), R = RR>,
        K: 'static,
        LD: 'static,
        LR: AddAssign<LR> + Mul<RR, Output = OR>,
        RD: 'static,
        RR: AddAssign<RR>,
        OR: AddAssign<OR>,
    > Op for Join<LC, RC>
{
    type D = (K, LD, RD);
    type R = OR;
}

impl<K: 'static, D: 'static, C: Op<D = (K, D)>> Relation<C> {
    pub fn join<C2: Op<D = (K, D2)>, D2: 'static, OR: AddAssign<OR>>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = (K, D, D2), R = OR>>
    where
        C::R: Mul<C2::R, Output = OR>,
    {
        Relation {
            inner: Join {
                _left: self.inner,
                _right: other.inner,
            },
        }
    }
}
