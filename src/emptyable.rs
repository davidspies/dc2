use std::collections::{BTreeMap, HashMap};

pub trait Emptyable: Default {
    fn is_empty(&self) -> bool;
}

impl<K: Ord, V> Emptyable for BTreeMap<K, V> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<K, V> Emptyable for HashMap<K, V> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl Emptyable for isize {
    fn is_empty(&self) -> bool {
        *self == 0
    }
}
