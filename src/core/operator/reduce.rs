use super::Op;
use crate::core::is_map::{IsAddMap, IsDiscardMap, IsMap, IsRemoveMap};
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::{ExecutionContext, Relation, Step};
use std::cell::{Ref, RefCell};
use std::collections::{hash_map, HashMap, HashSet};
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;

pub struct Reduce<D2, R2, C, K, M1, M2, F: Fn(&K, &M1) -> M2> {
    inner: C,
    input_maps: HashMap<K, M1>,
    output_maps: HashMap<K, M2>,
    proc: F,
    phantom: PhantomData<(D2, R2)>,
}

impl<
        C: Op<D = (K, D1)>,
        K: Key,
        D1,
        M1: IsAddMap<D1, C::R> + 'static,
        M2: IsMap<D2, R2> + 'static,
        MF: Fn(&K, &M1) -> M2 + 'static,
        D2: Key,
        R2: Monoid,
    > Reduce<D2, R2, C, K, M1, M2, MF>
{
    fn flow_inner<F: FnMut((K, D2), R2)>(&mut self, step: &Step, mut send: F, sending: bool) {
        let mut changed_keys = HashSet::new();
        let Reduce {
            inner, input_maps, ..
        } = self;
        inner.flow(step, |(k, x), r| {
            changed_keys.insert(k.clone());
            input_maps.add((k, x), r);
        });
        for k in changed_keys {
            let old_map = match self.input_maps.get(&k) {
                None => {
                    if let Some(om) = self.output_maps.remove(&k) {
                        om.into_discardable()
                    } else {
                        M2::Discardable::default()
                    }
                }
                Some(im) => {
                    let new_map = (self.proc)(&k, im);
                    let e = self.output_maps.entry(k.clone());
                    let (mut old_map, new_map_ref) = match e {
                        hash_map::Entry::Vacant(ve) => {
                            let new_map_ref: &M2 = ve.insert(new_map);
                            (M2::Discardable::default(), new_map_ref)
                        }
                        hash_map::Entry::Occupied(oe) => {
                            let new_map_ref = oe.into_mut();
                            (
                                mem::replace(new_map_ref, new_map).into_discardable(),
                                &*new_map_ref,
                            )
                        }
                    };
                    if sending {
                        new_map_ref.foreach(|x, r| {
                            let or = old_map.remove(x).unwrap_or_default();
                            let diff = r.clone() - or;
                            if !diff.is_zero() {
                                send((k.clone(), x.clone()), diff)
                            }
                        });
                    }
                    old_map
                }
            };
            if sending {
                for (x, r) in old_map.into_iter() {
                    send((k.clone(), x), -r)
                }
            }
        }
    }
}

impl<
        C: Op<D = (K, D1)>,
        K: Key,
        D1,
        M1: IsAddMap<D1, C::R> + 'static,
        M2: IsMap<D2, R2> + 'static,
        MF: Fn(&K, &M1) -> M2 + 'static,
        D2: Key,
        R2: Monoid,
    > Op for Reduce<D2, R2, C, K, M1, M2, MF>
{
    type D = (K, D2);
    type R = R2;

    fn flow<F: FnMut((K, D2), R2)>(&mut self, step: &Step, send: F) {
        self.flow_inner(step, send, true)
    }
}

impl<'a, K: Key, D: Key, C: Op<D = (K, D)>> Relation<'a, C> {
    pub fn reduce<
        D2: Key,
        R2: Monoid,
        MF: Fn(&K, &M1) -> M2 + 'static,
        M1: IsAddMap<D, C::R> + 'static,
        M2: IsMap<D2, R2> + 'static,
    >(
        self,
        proc: MF,
    ) -> Relation<'a, impl IsReduce<K = K, M = M2> + Op<D = (K, D2), R = R2>> {
        Relation {
            inner: Reduce {
                inner: self.inner,
                input_maps: HashMap::new(),
                output_maps: HashMap::new(),
                proc,
                phantom: PhantomData,
            },
            context_id: self.context_id,
            depth: self.depth,
            phantom: PhantomData,
        }
    }
}

pub trait IsReduce {
    type K;
    type M;
    fn read_ref<'a>(
        this: &'a RefCell<Self>,
        context: &'a ExecutionContext,
    ) -> Ref<'a, HashMap<Self::K, Self::M>>;
}

impl<
        K: Key,
        D1: Key,
        D2: Key,
        R2: Monoid,
        C: Op<D = (K, D1)>,
        F: Fn(&K, &M1) -> M2 + 'static,
        M1: IsAddMap<D1, C::R> + 'static,
        M2: IsMap<D2, R2> + 'static,
    > IsReduce for Reduce<D2, R2, C, K, M1, M2, F>
{
    type K = K;
    type M = M2;
    fn read_ref<'a>(
        this: &'a RefCell<Self>,
        context: &'a ExecutionContext,
    ) -> Ref<'a, HashMap<K, M2>> {
        this.borrow_mut()
            .flow_inner(&Step::Root(context.step), |_, _| {}, false);
        Ref::map(this.borrow(), |r| &r.output_maps)
    }
}

impl<'a, C: IsReduce> Relation<'a, C> {
    pub fn get_reduce_output(self) -> impl ReduceOutput<K = C::K, M = C::M> {
        RefCell::new(self.inner)
    }
}

pub trait ReduceOutput {
    type K;
    type M;
    fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<Self::K, Self::M>>;
}

impl<T: ReduceOutput> ReduceOutput for Box<T> {
    type K = T::K;
    type M = T::M;
    fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<T::K, T::M>> {
        <Box<T> as Deref>::deref(self).read(context)
    }
}

impl<C: IsReduce> ReduceOutput for RefCell<C> {
    type K = C::K;
    type M = C::M;
    fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<C::K, C::M>> {
        C::read_ref(self, context)
    }
}
