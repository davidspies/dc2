use super::IsAddMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use std::collections::HashMap;
use std::mem;

#[derive(Clone)]
pub struct HybridMap<D, R> {
    hashed: HashMap<D, R>,
    pending: Vec<(D, R)>,
    threshold: usize,
}

impl<D, R> HybridMap<D, R> {
    pub fn new() -> Self {
        HybridMap {
            hashed: HashMap::new(),
            pending: Vec::new(),
            threshold: 16,
        }
    }
    pub fn steal(&mut self) -> Vec<(D, R)> {
        let mut res: Vec<(D, R)> = mem::take(&mut self.pending);
        res.extend(mem::take(&mut self.hashed));
        res
    }
}

impl<D, R> Default for HybridMap<D, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: Key, R: Monoid> IsAddMap<D, R> for HybridMap<D, R> {
    fn add(&mut self, x: D, r: R) {
        self.pending.push((x, r));
        if self.pending.len() >= self.threshold {
            for (x, r) in mem::take(&mut self.pending) {
                self.hashed.add(x, r);
            }
            if self.hashed.len() >= self.threshold {
                self.threshold *= 2;
            }
        }
    }
}
