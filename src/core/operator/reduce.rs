use super::barrier::Barrier;
use super::split::{Receiver, SourceRef};
use super::Op;
use crate::core::is_map::{IsAddMap, IsDiscardMap, IsMap, IsRemoveMap};
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::node::Node;
use crate::core::{ContextId, CreationContext, ExecutionContext, Relation, Step};
use std::cell::{Ref, RefCell};
use std::collections::{hash_map, HashMap, HashSet};
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;

pub struct Reduce<D2, R2, C, K, M1, M2, F: Fn(&K, &M1) -> M2> {
    inner: Node<C>,
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
    > Op for Reduce<D2, R2, C, K, M1, M2, MF>
{
    type D = (K, D2);
    type R = R2;

    fn default_op_name() -> &'static str {
        "reduce"
    }
    fn flow<F: FnMut((K, D2), R2)>(&mut self, step: Step, mut send: F) {
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
                    new_map_ref.foreach(|x, r| {
                        let or = old_map.remove(x).unwrap_or_default();
                        let diff = r.clone() - or;
                        if !diff.is_zero() {
                            send((k.clone(), x.clone()), diff)
                        }
                    });
                    old_map
                }
            };
            for (x, r) in old_map.into_iter() {
                send((k.clone(), x), -r)
            }
        }
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
        Relation::new(
            vec![self.dep()],
            Reduce {
                inner: self.inner,
                input_maps: HashMap::new(),
                output_maps: HashMap::new(),
                proc,
                phantom: PhantomData,
            },
        )
    }
}

pub trait IsReduce {
    type K;
    type M;
    fn get_ref(&self) -> &HashMap<Self::K, Self::M>;
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
    fn get_ref<'a>(&'a self) -> &'a HashMap<K, M2> {
        &self.output_maps
    }
}

impl<C: IsReduce + Op> Relation<'static, C> {
    pub fn split_reduce_output(
        self,
        context: &CreationContext,
    ) -> (
        Relation<'static, Receiver<C>>,
        impl ReduceOutput<K = C::K, M = C::M>,
    ) {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        let context_id = self.context_id;
        let r = self.split();
        let inner = r.inner.inner.get_source_ref();
        (r, SplitReduceOutputImpl { context_id, inner })
    }
    pub fn reduce_output(self, context: &CreationContext) -> impl ReduceOutput<K = C::K, M = C::M> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        let context_id = self.context_id;
        let r = self.barrier();
        ReduceOutputImpl {
            context_id,
            inner: RefCell::new(r.inner),
        }
    }
}

pub struct SplitReduceOutputImpl<C: Op> {
    context_id: ContextId,
    inner: SourceRef<C>,
}

pub struct ReduceOutputImpl<C: Op> {
    context_id: ContextId,
    inner: RefCell<Node<Barrier<C>>>,
}

impl<C: IsReduce + Op> ReduceOutput for SplitReduceOutputImpl<C> {
    type K = C::K;
    type M = C::M;

    fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<C::K, C::M>> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        self.inner.propagate(context.step);
        Ref::map(self.inner.get_inner(), |n| IsReduce::get_ref(&n.inner))
    }
}

impl<C: IsReduce + Op> ReduceOutput for ReduceOutputImpl<C> {
    type K = C::K;
    type M = C::M;

    fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<C::K, C::M>> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        if self.inner.borrow().inner.dirty(context.step) {
            self.inner.borrow_mut().flow(context.step, |_, _| ());
        }
        Ref::map(self.inner.borrow(), |n| n.inner.inner.inner.get_ref())
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
