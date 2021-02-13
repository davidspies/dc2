use super::{default_flow_to, DynOperator, Operator};
use crate::is_map::{IsAddMap, IsDiscardMap, IsMap, IsRemoveMap};
use crate::key::Key;
use crate::monoid::Monoid;
use crate::Relation;
use crate::Step;
use std::collections::{hash_map, HashMap, HashSet};
use std::marker::PhantomData;
use std::mem;

pub(crate) struct Reduce<D2, R2, C, K, M1, M2, F: Fn(K, &M1) -> M2> {
    inner: C,
    input_maps: HashMap<K, M1>,
    output_maps: HashMap<K, M2>,
    proc: F,
    phantom: PhantomData<(D2, R2)>,
}

impl<
        C: Operator<D = (K, D1)>,
        K: Key,
        D1,
        M1: IsAddMap<D1, C::R>,
        M2: IsMap<D2, R2>,
        MF: Fn(K, &M1) -> M2,
        D2: Key,
        R2: Monoid,
    > DynOperator for Reduce<D2, R2, C, K, M1, M2, MF>
{
    type D = (K, D2);
    type R = R2;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}

impl<
        C: Operator<D = (K, D1)>,
        K: Key,
        D1,
        M1: IsAddMap<D1, C::R>,
        M2: IsMap<D2, R2>,
        MF: Fn(K, &M1) -> M2,
        D2: Key,
        R2: Monoid,
    > Operator for Reduce<D2, R2, C, K, M1, M2, MF>
{
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
                    let new_map = (self.proc)(k.clone(), im);
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

impl<K: Key, D: Key, C: Operator<D = (K, D)>> Relation<C> {
    pub fn reduce<
        D2: Key,
        R2: Monoid,
        F: Fn(K, &M1) -> M2,
        M1: IsAddMap<D, C::R>,
        M2: IsMap<D2, R2>,
    >(
        self,
        proc: F,
    ) -> Relation<impl Operator<D = (K, D2), R = R2>> {
        Relation {
            inner: Reduce {
                inner: self.inner,
                input_maps: HashMap::new(),
                output_maps: HashMap::new(),
                proc,
                phantom: PhantomData,
            },
            context_id: self.context_id,
        }
    }
}
