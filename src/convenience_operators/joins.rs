use crate::key::Key;
use crate::monoid::Monoid;
use crate::tuple::{fst, snd, swap};
use crate::{Op, Relation};
use std::ops::Mul;

impl<'a, C: Op> Relation<C> {
    pub fn semijoin_on<F: Fn(&C::D) -> C2::D + 'static, C2: Op, R3: Monoid>(
        self,
        other: Relation<C2>,
        f: F,
    ) -> Relation<impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<C2::R, Output = R3>,
    {
        self.hmap(move |val| (f(&val), val))
            .semijoin(other)
            .op_named("semijoin_on")
            .hmap(snd)
    }
    pub fn antijoin_on<F: Fn(&C::D) -> C2::D + 'static, C2: Op>(
        self,
        other: Relation<C2>,
        f: F,
    ) -> Relation<impl Op<D = C::D, R = C::R>>
    where
        C::R: Mul<C2::R, Output = C::R>,
    {
        self.hmap(move |val| (f(&val), val))
            .antijoin(other)
            .op_named("antijoin_on")
            .hmap(snd)
    }
    pub fn intersection<C2: Op<D = C::D>, R3: Monoid>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<C2::R, Output = R3>,
    {
        self.hmap(|x| (x, ()))
            .semijoin(other)
            .op_named("intersection")
            .hmap(fst)
    }
    pub fn set_minus<C2: Op<D = C::D>>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = C::D, R = C::R>>
    where
        C::R: Mul<C2::R, Output = C::R>,
    {
        self.hmap(|x| (x, ()))
            .antijoin(other)
            .op_named("set_minus")
            .hmap(fst)
    }
}

impl<'a, K: Key, V: Key, C: Op<D = (K, V)>> Relation<C> {
    pub fn semijoin<C2: Op<D = K, R = R2>, R2: Monoid, R3: Monoid>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<R2, Output = R3>,
    {
        self.join(other.hmap(|x| (x, ())))
            .op_named("semijoin")
            .hmap(|(k, (x, ()))| (k, x))
    }
    pub fn semijoin_on_fst<C2: Op<D = K, R = R2>, R2: Monoid, R3: Monoid>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<R2, Output = R3>,
    {
        self.semijoin(other).op_named("semijoin_on_fst")
    }
    pub fn semijoin_on_snd<C2: Op<D = V, R = R2>, R2: Monoid, R3: Monoid>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = C::D, R = R3>>
    where
        C::R: Mul<R2, Output = R3>,
    {
        self.hmap(swap)
            .semijoin(other)
            .op_named("semijoin_on_snd")
            .hmap(swap)
    }
    pub fn antijoin_on_fst<C2: Op<D = K, R = R2>, R2: Monoid>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = C::D, R = C::R>>
    where
        C::R: Mul<R2, Output = C::R>,
    {
        self.antijoin(other).op_named("antijoin_on_fst")
    }
    pub fn antijoin_on_snd<C2: Op<D = V, R = R2>, R2: Monoid>(
        self,
        other: Relation<C2>,
    ) -> Relation<impl Op<D = C::D, R = C::R>>
    where
        C::R: Mul<R2, Output = C::R>,
    {
        self.hmap(swap)
            .antijoin(other)
            .op_named("antijoin_on_snd")
            .hmap(swap)
    }
}
