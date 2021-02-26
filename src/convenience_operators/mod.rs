mod joins;

use crate::core::iter::TupleableWith;
use crate::key::Key;
use crate::map::{SingletonMap, UnitMap};
use crate::monoid::Monoid;
use crate::{
    Arrangement, CreationContext, DynOp, ExecutionContext, Input, IsReduce, Op, Receiver,
    ReduceOutput, Relation,
};
use std::collections::{BTreeMap, HashMap};
use std::iter;
use std::ops::Neg;

pub type DynReceiver<D, R = isize> = Receiver<DynOp<D, R>>;
pub type Collection<'a, D, R = isize> = Relation<'a, DynReceiver<D, R>>;
pub type MapMapArrangement<K, V, R = isize> = Arrangement<(K, V), R, HashMap<K, HashMap<V, R>>>;
pub type OrderedArrangement<K, V, R = isize> = Arrangement<(K, V), R, BTreeMap<K, HashMap<V, R>>>;
pub type MappingArrangement<K, V> = Box<dyn ReduceOutput<K = K, M = SingletonMap<V>>>;

impl<'a, C: Op> Relation<'a, C> {
    pub fn get_dyn_arrangement(self, context: &CreationContext) -> Arrangement<C::D, C::R>
    where
        'a: 'static,
    {
        self.dynamic().get_arrangement(context)
    }
    /// Convenience function equivalent to `self.dynamic().split()`.
    pub fn collect(self) -> Collection<'a, C::D, C::R> {
        self.dynamic().split()
    }
    pub fn flat_map<F: Fn(C::D) -> I + 'static, D2: Key, I: IntoIterator<Item = D2>>(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = D2, R = C::R>> {
        self.flat_map_dr(move |x, r| f(x).into_iter().tuple_with(r))
            .op_named("flat_map")
    }
    pub fn map<F: Fn(C::D) -> D2 + 'static, D2: Key>(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = D2, R = C::R>> {
        self.flat_map(move |x| iter::once(f(x))).op_named("map")
    }
    pub fn hmap<F: Fn(C::D) -> D2 + 'static, D2: Key>(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = D2, R = C::R>> {
        self.map(f).hidden()
    }
    pub fn filter<F: Fn(&C::D) -> bool + 'static>(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>> {
        self.flat_map(move |x| if f(&x) { Some(x) } else { None })
            .op_named("filter")
    }
    pub fn map_r<F: Fn(C::R) -> R2 + 'static, R2: Monoid>(
        self,
        f: F,
    ) -> Relation<'a, impl Op<D = C::D, R = R2>> {
        self.flat_map_dr(move |x, r| iter::once((x, f(r))))
            .op_named("map_r")
    }
    pub fn negate(self) -> Relation<'a, impl Op<D = C::D, R = C::R>> {
        self.map_r(Neg::neg).op_named("negate")
    }
    pub fn counts(
        self,
    ) -> Relation<
        'a,
        impl Op<D = (C::D, C::R), R = isize> + IsReduce<K = C::D, M = SingletonMap<C::R>>,
    >
    where
        C::R: Key,
    {
        self.hmap(|x| (x, ()))
            .reduce(|_, xs: &UnitMap<C::R>| SingletonMap(xs.0.clone()))
            .op_named("counts")
    }
    /// Equivalent to `self.barrier()`. For performance reasons, this should generally be called
    /// on inputs to subgraphs which sit on top of long dependency chains.
    ///
    /// Example:
    ///
    /// ```
    /// use dc2::{CreationContext, Op, Relation};
    /// let mut creation = CreationContext::new();
    /// let (foo_inp, foo) = creation.create_input::<(usize, usize), isize>();
    /// # let (bar_inp, some_big_complicated_relation) = creation.create_input::<(usize, usize), isize>();
    /// // let bar: Relation<'static, impl Op<D=(usize, usize), R=isize>>
    /// let bar = some_big_complicated_relation;
    /// let mut subcontext = creation.subgraph::<usize>();
    /// let (v, vrel) = subcontext.variable::<usize, isize>();
    /// let next = foo.concat(vrel.join(bar.enter()).map(|(k, (x, y))| (k + 1, x + y))).split();
    /// v.set(next.clone());
    /// let result = next.leave(&subcontext.finish());
    /// ```
    ///
    /// Here we call `bar.enter()` because bar sits on top of a big complicated relation, but we
    /// don't bother with `foo.enter()` since `foo` comes directly from an input (similarly calls
    /// to `split` can be thought of as an "input" for this purpose since `split` calls
    /// `self.barrier()`).
    pub fn enter(self) -> Relation<'a, impl Op<D = C::D, R = C::R>> {
        self.barrier().op_named("enter")
    }
    pub fn distinct(self) -> Relation<'a, impl Op<D = C::D, R = isize>> {
        self.hmap(|x| (x, ()))
            .reduce(|_, _: &UnitMap<C::R>| UnitMap(1))
            .op_named("distinct")
            .hmap(|(k, ())| k)
    }
}

impl<'a, C: Op<R = isize>> Relation<'a, C> {
    pub fn hist_including<C2: Op<D = C::D, R = isize>>(
        self,
        keys: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = (C::D, isize), R = isize>> {
        self.concat(keys).counts().hmap(|(k, v)| (k, v - 1))
    }
    pub fn histogram<C2: Clone + Op<D = C::D, R = isize>>(
        self,
        keys: Relation<'a, C2>,
    ) -> Relation<'a, impl Op<D = (C::D, isize), R = isize>> {
        self.intersection(keys.clone()).hist_including(keys)
    }
}

impl<'a, K: Key, V: Key, C: Op<D = (K, V)>> Relation<'a, C> {
    pub fn group_min(
        self,
    ) -> Relation<'a, impl Op<D = C::D, R = isize> + IsReduce<K = K, M = SingletonMap<V>>>
    where
        V: Ord,
    {
        self.reduce(|_, xs: &BTreeMap<V, C::R>| {
            SingletonMap(xs.first_key_value().unwrap().0.clone())
        })
        .op_named("group_min")
    }
    pub fn group_max(
        self,
    ) -> Relation<'a, impl Op<D = C::D, R = isize> + IsReduce<K = K, M = SingletonMap<V>>>
    where
        V: Ord,
    {
        self.reduce(|_, xs: &BTreeMap<V, C::R>| {
            SingletonMap(xs.last_key_value().unwrap().0.clone())
        })
        .op_named("group_max")
    }
}

impl<K: Key, V: Key, C: Op<D = (K, V), R = isize>> Relation<'static, C> {
    pub fn assert_1to1_with_output(
        self,
        context: &CreationContext,
    ) -> (
        Relation<'static, impl Op<D = C::D, R = isize>>,
        impl ReduceOutput<K = K, M = SingletonMap<V>>,
    ) {
        self.reduce(|_, m: &HashMap<V, C::R>| {
            let mut iter = m.iter();
            match iter.next() {
                None => panic!("Empty map"),
                Some((v, &r)) => {
                    assert!(iter.next().is_none());
                    assert_eq!(r, 1);
                    SingletonMap(v.clone())
                }
            }
        })
        .op_named("assert_1to1")
        .hidden()
        .split_reduce_output(context)
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
