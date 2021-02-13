use super::Op;
use crate::core::borrow::BorrowOrDefault;
use crate::core::is_map::IsAddMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::{Relation, Step};
use std::collections::HashMap;
use std::ops::Mul;

struct Join<LC, RC, K, LD, LR, RD, RR> {
    left: LC,
    right: RC,
    left_map: HashMap<K, HashMap<LD, LR>>,
    right_map: HashMap<K, HashMap<RD, RR>>,
}

impl<
        LC: Op<D = (K, LD), R = LR>,
        RC: Op<D = (K, RD), R = RR>,
        K: Key,
        LD: Key,
        LR: Monoid + Mul<RR, Output = OR>,
        RD: Key,
        RR: Monoid,
        OR: Monoid,
    > Op for Join<LC, RC, K, LD, LR, RD, RR>
{
    type D = (K, LD, RD);
    type R = OR;

    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: Step, mut send: F) {
        let Join {
            left,
            right,
            left_map,
            right_map,
        } = self;
        left.flow(step, |(k, lx), lr| {
            for (rx, rr) in right_map.get(&k).borrow_or_default().iter() {
                send((k.clone(), lx.clone(), rx.clone()), lr.clone() * rr.clone());
            }
            left_map.add((k, lx), lr);
        });
        right.flow(step, |(k, rx), rr| {
            for (lx, lr) in left_map.get(&k).borrow_or_default().iter() {
                send((k.clone(), lx.clone(), rx.clone()), lr.clone() * rr.clone());
            }
            right_map.add((k, rx), rr);
        });
    }
}

impl<K: Key, D: Key, C: Op<D = (K, D)>> Relation<C> {
    pub fn join<C2: Op<D = (K, D2)>, D2: Key, OR: Monoid>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = (K, D, D2), R = OR>>
    where
        C::R: Mul<C2::R, Output = OR>,
    {
        assert_eq!(self.context_id, other.context_id, "Context mismatch");
        Relation {
            inner: Join {
                left: self.inner,
                right: other.inner,
                left_map: HashMap::new(),
                right_map: HashMap::new(),
            },
            context_id: self.context_id,
        }
    }
}
