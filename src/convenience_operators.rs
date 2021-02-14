use crate::core::iter::TupleableWith;
use crate::key::Key;
use crate::map::{SingletonMap, UnitMap};
use crate::monoid::Monoid;
use crate::{Arrangement, CreationContext, DynOp, ExecutionContext, Input, Op, Receiver, Relation};
use std::collections::BTreeMap;
use std::iter;
use std::ops::{Mul, Neg};

pub type DynReceiver<D, R = isize> = Receiver<DynOp<D, R>>;
pub type Collection<'a, D, R = isize> = Relation<'a, DynReceiver<D, R>>;

impl<'a, C: Op> Relation<'a, C> {
    pub fn get_dyn_arrangement(self, context: &CreationContext) -> Arrangement<C::D, C::R>
    where
        'a: 'static,
        C: 'static,
    {
        self.dynamic().get_arrangement(context)
    }
    pub fn collect(self) -> Collection<'a, C::D, C::R>
    where
        C: 'static,
    {
        self.dynamic().split()
    }
    pub fn flat_map<F: Fn(C::D) -> I + 'static, D2: Key, I: IntoIterator<Item = D2>>(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = D2, R = C::R>> {
        self.flat_map_dr(move |x, r| f(x).into_iter().tuple_with(r))
    }
    pub fn map<F: Fn(C::D) -> D2 + 'static, D2: Key>(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = D2, R = C::R>> {
        self.flat_map(move |x| iter::once(f(x)))
    }
    pub fn filter<F: Fn(&C::D) -> bool + 'static>(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>> {
        self.flat_map(move |x| if f(&x) { Some(x) } else { None })
    }
    pub fn map_r<F: Fn(C::R) -> R2 + 'static, R2: Monoid>(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = C::D, R = R2>> {
        self.flat_map_dr(move |x, r| iter::once((x, f(r))))
    }
    pub fn negate(self) -> Relation<'a, impl Op<D = C::D, R = C::R>> {
        self.map_r(Neg::neg)
    }
    pub fn counts(self) -> Relation<'a, impl Op<D = (C::D, C::R), R = isize>>
    where
        C::R: Key,
    {
        self.map(|x| (x, ()))
            .reduce(|_, xs: &UnitMap<C::R>| SingletonMap(xs.0.clone()))
    }
    pub fn enter(self) -> Relation<'a, impl Op<D = C::D, R = C::R>> {
        self.barrier()
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
        self.join(other.map(|x| (x, ()))).map(|(k, x, ())| (k, x))
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
    pub fn group_min(self) -> Relation<'a, impl Op<D = C::D, R = isize>>
    where
        V: Ord,
    {
        self.reduce(|_, xs: &BTreeMap<V, C::R>| {
            SingletonMap(xs.first_key_value().unwrap().0.clone())
        })
    }
    pub fn group_max(self) -> Relation<'a, impl Op<D = C::D, R = isize>>
    where
        V: Ord,
    {
        self.reduce(|_, xs: &BTreeMap<V, C::R>| {
            SingletonMap(xs.last_key_value().unwrap().0.clone())
        })
    }
}

impl<D: Key> Input<D> {
    pub fn insert(&self, context: &ExecutionContext, x: D) {
        self.update(context, x, 1)
    }
    pub fn delete(&self, context: &ExecutionContext, x: D) {
        self.update(context, x, -1)
    }
}
