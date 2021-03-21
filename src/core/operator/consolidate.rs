use super::Op;
use crate::core::is_map::IsAddMap;
use crate::core::node::Node;
use crate::core::{Relation, Step};
use std::collections::HashMap;

struct Consolidate<C> {
    inner: Node<C>,
}

impl<C: Op> Op for Consolidate<C> {
    type D = C::D;
    type R = C::R;

    fn default_op_name() -> &'static str {
        "consolidate"
    }
    fn flow<F: FnMut(C::D, C::R)>(&mut self, step: &Step, mut send: F) {
        let mut m = HashMap::new();
        self.inner.flow(step, |x, r| m.add(x, r));
        for (x, r) in m {
            send(x, r)
        }
    }
}

impl<'a, C: Op> Relation<'a, C> {
    pub fn consolidate(self) -> Relation<'a, impl Op<D = C::D, R = C::R>> {
        Relation::new(vec![self.dep()], Consolidate { inner: self.inner })
    }
}
