use std::hash::Hash;

pub trait Key: Clone + Eq + Hash + 'static {}

impl<K: Clone + Eq + Hash + 'static> Key for K {}
