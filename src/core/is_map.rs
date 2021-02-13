use crate::core::emptyable::Emptyable;
use crate::core::monoid::Monoid;
use std::collections::{btree_map, hash_map, BTreeMap, HashMap};
use std::hash::Hash;

pub trait IsRemoveMap<K, V>: Emptyable {
    fn remove(&mut self, k: &K) -> Option<V>;
}

pub trait IsDiscardMap<K, V>: IsRemoveMap<K, V> {
    type Iter: Iterator<Item = (K, V)>;
    fn into_iter(self) -> Self::Iter;
}

pub trait IsMap<K, V> {
    type Discardable: IsDiscardMap<K, V>;

    fn foreach<F: FnMut(&K, &V)>(&self, op: F);
    fn into_discardable(self) -> Self::Discardable;
}

pub trait IsAddMap<K, V>: Emptyable {
    fn add(&mut self, k: K, v: V);
}

impl<K: Eq + Hash, V> IsRemoveMap<K, V> for HashMap<K, V> {
    fn remove(&mut self, k: &K) -> Option<V> {
        self.remove(k)
    }
}

impl<K: Eq + Hash, V> IsMap<K, V> for HashMap<K, V> {
    type Discardable = Self;

    fn foreach<F: FnMut(&K, &V)>(&self, mut op: F) {
        for (k, v) in self.iter() {
            op(k, v)
        }
    }
    fn into_discardable(self) -> Self {
        self
    }
}
impl<K: Eq + Hash, V> IsDiscardMap<K, V> for HashMap<K, V> {
    type Iter = hash_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::Iter {
        IntoIterator::into_iter(self)
    }
}
impl<K: Eq + Hash, V: Monoid> IsAddMap<K, V> for HashMap<K, V> {
    fn add(&mut self, k: K, v: V) {
        let e = self.entry(k);
        match e {
            hash_map::Entry::Occupied(mut occ) => {
                let is_zero = {
                    let r = occ.get_mut();
                    *r += v;
                    r.is_zero()
                };
                if is_zero {
                    occ.remove();
                }
            }
            hash_map::Entry::Vacant(vac) => {
                if !v.is_zero() {
                    vac.insert(v);
                }
            }
        }
    }
}

impl<K1: Eq + Hash, K2, V: Monoid, M2: IsAddMap<K2, V> + Emptyable> IsAddMap<(K1, K2), V>
    for HashMap<K1, M2>
{
    fn add(&mut self, (k1, k2): (K1, K2), v: V) {
        if v.is_zero() {
            return;
        }
        let e = self.entry(k1);
        match e {
            hash_map::Entry::Occupied(mut occ) => {
                let m = occ.get_mut();
                m.add(k2, v);
                if m.is_empty() {
                    occ.remove();
                }
            }
            hash_map::Entry::Vacant(vac) => {
                let m = vac.insert(Default::default());
                m.add(k2, v)
            }
        }
    }
}

impl<K1: Ord, K2, V: Monoid, M2: IsAddMap<K2, V> + Emptyable> IsAddMap<(K1, K2), V>
    for BTreeMap<K1, M2>
{
    fn add(&mut self, (k1, k2): (K1, K2), v: V) {
        if v.is_zero() {
            return;
        }
        let e = self.entry(k1);
        match e {
            btree_map::Entry::Occupied(mut occ) => {
                let m = occ.get_mut();
                m.add(k2, v);
                if m.is_empty() {
                    occ.remove();
                }
            }
            btree_map::Entry::Vacant(vac) => {
                let m = vac.insert(Default::default());
                m.add(k2, v)
            }
        }
    }
}
