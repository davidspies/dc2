use crate::key::Key;
use crate::monoid::Monoid;
use crate::tuple::{fst, snd, swap};
use crate::{Op, Relation};
use std::ops::Mul;

impl<'a, C: Op> Relation<'a, C> {
    pub fn semijoin_on<F: Fn(&C::D) -> C2::D + 'static, C2: Op, R3: Monoid>(
        self,
        other: Relation<'a, C2>,
        f: F,
    ) -> Relation<'a, impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<C2::R, Output = R3>,
    {
        self.map(move |val| (f(&val), val)).semijoin(other).map(snd)
    }
    pub fn antijoin_on<F: Fn(&C::D) -> C2::D + 'static, C2: Op>(
        self,
        other: Relation<'a, C2>,
        f: F,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>>
    where
        C::R: Mul<C2::R, Output = C::R>,
    {
        self.map(move |val| (f(&val), val)).antijoin(other).map(snd)
    }
    pub fn intersection<C2: Op<D = C::D>, R3: Monoid>(
        self,
        other: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<C2::R, Output = R3>,
    {
        self.map(|x| (x, ())).semijoin(other).map(fst)
    }
    pub fn set_minus<C2: Op<D = C::D>>(
        self,
        other: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>>
    where
        C::R: Mul<C2::R, Output = C::R>,
    {
        self.map(|x| (x, ())).antijoin(other).map(fst)
    }
}

impl<'a, K: Key, V: Key, C: Op<D = (K, V)>> Relation<'a, C> {
    pub fn semijoin<C2: Op<D = K, R = R2>, R2: Monoid, R3: Monoid>(
        self,
        other: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<R2, Output = R3>,
    {
        self.join(other.map(|x| (x, ()))).map(|(k, (x, ()))| (k, x))
    }
    pub fn semijoin_on_fst<C2: Op<D = K, R = R2>, R2: Monoid, R3: Monoid>(
        self,
        other: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<R2, Output = R3>,
    {
        self.semijoin(other)
    }
    pub fn semijoin_on_snd<C2: Op<D = V, R = R2>, R2: Monoid, R3: Monoid>(
        self,
        other: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<R2, Output = R3>,
    {
        self.map(swap).semijoin(other).map(swap)
    }
    pub fn antijoin<C2: Op<D = K, R = R2>, R2: Monoid>(
        self,
        other: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>>
    where
        C::R: Mul<R2, Output = C::R>,
    {
        let this = self.split();
        this.clone().concat(this.semijoin(other).negate())
    }
    pub fn antijoin_on_fst<C2: Op<D = K, R = R2>, R2: Monoid>(
        self,
        other: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>>
    where
        C::R: Mul<R2, Output = C::R>,
    {
        self.antijoin(other)
    }
    pub fn antijoin_on_snd<C2: Op<D = V, R = R2>, R2: Monoid>(
        self,
        other: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>>
    where
        C::R: Mul<R2, Output = C::R>,
    {
        self.map(swap).antijoin(other).map(swap)
    }
}
