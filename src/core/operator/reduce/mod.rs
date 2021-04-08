mod output;

pub use self::output::{ReduceOutput, ReduceOutputImpl, SplitReduceOutputImpl};
use super::Op;
use crate::core::is_map::{IsAddMap, IsDiscardMap, IsMap, IsRemoveMap};
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::node::Node;
use crate::core::{Relation, Step};
use std::collections::{hash_map, HashMap, HashSet};
use std::marker::PhantomData;
use std::mem;

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
                None => match self.output_maps.remove(&k) {
                    Some(om) => om.into_discardable(),
                    None => M2::Discardable::default(),
                },
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

impl<K: Key, D: Key, C: Op<D = (K, D)>> Relation<C> {
    pub fn reduce<
        D2: Key,
        R2: Monoid,
        MF: Fn(&K, &M1) -> M2 + 'static,
        M1: IsAddMap<D, C::R> + 'static,
        M2: IsMap<D2, R2> + 'static,
    >(
        self,
        proc: MF,
    ) -> Relation<impl IsReduce<K = K, M = M2> + Op<D = (K, D2), R = R2>> {
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
