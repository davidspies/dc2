pub use crate::core::is_map::{IsAddMap, IsDiscardMap, IsMap, IsRemoveMap};
use crate::emptyable::Emptyable;
use crate::monoid::Monoid;
use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap, HashMap};
use std::hash::Hash;
use std::mem;
use std::option;

#[derive(Clone, Copy)]
pub struct OptionMap<K, V = isize>(Option<(K, V)>);
#[derive(Clone, Copy)]
pub struct SingletonMap<K>(pub K);
pub struct UnitMap<V>(pub V);

impl<K, V: Monoid> OptionMap<K, V> {
    pub fn new(k: K, v: V) -> Self {
        OptionMap(if v.is_zero() { None } else { Some((k, v)) })
    }
    pub fn as_ref(&self) -> Option<&(K, V)> {
        self.0.as_ref()
    }
}

impl<K, V> Default for OptionMap<K, V> {
    fn default() -> Self {
        OptionMap(None)
    }
}

impl<K, V> Emptyable for OptionMap<K, V> {
    fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

impl<K: Eq, V> IsRemoveMap<K, V> for OptionMap<K, V> {
    fn remove(&mut self, k: &K) -> Option<V> {
        let (k2, _) = self.0.as_ref()?;
        if k2 == k {
            self.0.take().map(|(_, v)| v)
        } else {
            None
        }
    }
}

impl<K: Eq, V> IsMap<K, V> for OptionMap<K, V> {
    type Discardable = Self;

    fn foreach<F: FnMut(&K, &V)>(&self, mut op: F) {
        self.0.as_ref().map(|(k, v)| op(k, v));
    }
    fn into_discardable(self) -> Self {
        self
    }
}

impl<K: Eq> IsMap<K, isize> for SingletonMap<K> {
    type Discardable = OptionMap<K, isize>;
    fn foreach<F: FnMut(&K, &isize)>(&self, mut op: F) {
        op(&self.0, &1)
    }
    fn into_discardable(self) -> Self::Discardable {
        OptionMap(Some((self.0, 1)))
    }
}

pub struct VecMap<K, V>(Vec<(K, V)>);

impl<K: Ord, V> VecMap<K, V> {
    pub fn new(mut xs: Vec<(K, V)>) -> Self {
        xs.sort_unstable_by(|l, r| match l.0.cmp(&r.0) {
            Ordering::Equal => panic!("Duplicate keys"),
            r => r,
        });
        VecMap(xs)
    }
}
impl<K, V> Default for VecMap<K, V> {
    fn default() -> Self {
        VecMap(Vec::new())
    }
}
impl<K, V> Emptyable for VecMap<K, V> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
impl<K: Ord, V: Emptyable> IsRemoveMap<K, V> for VecMap<K, V> {
    fn remove(&mut self, k: &K) -> Option<V> {
        let i = match self.0.binary_search_by_key(&k, |(k, _)| k) {
            Ok(i) => i,
            Err(_) => return None,
        };
        let (_, ref mut v) = &mut self.0[i];
        if v.is_empty() {
            None
        } else {
            Some(mem::take(v))
        }
    }
}
impl<K: Ord, V: Emptyable> IsDiscardMap<K, V> for VecMap<K, V> {
    type Iter = <Vec<(K, V)> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::Iter {
        self.0.into_iter()
    }
}

impl<K: Ord, V: Emptyable> IsMap<K, V> for VecMap<K, V> {
    type Discardable = Self;
    fn foreach<F: FnMut(&K, &V)>(&self, mut op: F) {
        for (k, v) in self.0.iter() {
            op(k, v)
        }
    }
    fn into_discardable(self) -> Self {
        self
    }
}

impl<V> IsMap<(), V> for UnitMap<V> {
    type Discardable = OptionMap<(), V>;
    fn foreach<F: FnMut(&(), &V)>(&self, mut op: F) {
        op(&(), &self.0)
    }
    fn into_discardable(self) -> Self::Discardable {
        OptionMap(Some(((), self.0)))
    }
}

impl<K: Eq, V> IsDiscardMap<K, V> for OptionMap<K, V> {
    type Iter = option::IntoIter<(K, V)>;

    fn into_iter(self) -> Self::Iter {
        self.0.into_iter()
    }
}

impl<V: Default> Default for UnitMap<V> {
    fn default() -> Self {
        UnitMap(Default::default())
    }
}

impl<V: Emptyable> Emptyable for UnitMap<V> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<V: Monoid> IsAddMap<(), V> for UnitMap<V> {
    fn add(&mut self, (): (), v: V) {
        self.0 += v;
    }
}

impl<K: Ord, V: Monoid> IsAddMap<K, V> for BTreeMap<K, V> {
    fn add(&mut self, k: K, v: V) {
        let e = self.entry(k);
        match e {
            btree_map::Entry::Occupied(mut occ) => {
                let is_zero = {
                    let r = occ.get_mut();
                    *r += v;
                    r.is_zero()
                };
                if is_zero {
                    occ.remove();
                }
            }
            btree_map::Entry::Vacant(vac) => {
                if !v.is_zero() {
                    vac.insert(v);
                }
            }
        }
    }
}
impl<K: Ord, V> IsRemoveMap<K, V> for BTreeMap<K, V> {
    fn remove(&mut self, k: &K) -> Option<V> {
        self.remove(k)
    }
}

impl<K: Ord, V> IsMap<K, V> for BTreeMap<K, V> {
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

impl<K: Ord, V> IsDiscardMap<K, V> for BTreeMap<K, V> {
    type Iter = btree_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::Iter {
        IntoIterator::into_iter(self)
    }
}

impl<K1: Eq + Hash, K2, V, M: IsRemoveMap<K2, V>> IsRemoveMap<(K1, K2), V> for HashMap<K1, M> {
    fn remove(&mut self, (k1, k2): &(K1, K2)) -> Option<V> {
        let m = self.get_mut(k1)?;
        let v = m.remove(k2)?;
        if m.is_empty() {
            self.remove(k1);
        }
        Some(v)
    }
}

pub trait AssertOnes {
    type Result;

    fn assert_ones(self) -> Self::Result;
}

pub trait HasOne {
    fn is_one(&self) -> bool;
}

impl HasOne for isize {
    fn is_one(&self) -> bool {
        *self == 1
    }
}

impl HasOne for &isize {
    fn is_one(&self) -> bool {
        **self == 1
    }
}

impl<K, R: HasOne, I: Iterator<Item = (K, R)>> AssertOnes for I {
    type Result = impl Iterator<Item = K>;

    fn assert_ones(self) -> Self::Result {
        self.map(|(k, r)| if r.is_one() { k } else { panic!("Not a one") })
    }
}
