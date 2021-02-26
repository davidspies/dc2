use super::Op;
use crate::core::is_map::IsAddMap;
use crate::core::node::Node;
use crate::core::{Relation, Step};
use std::collections::HashMap;
use std::marker::PhantomData;

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
        Relation {
            inner: self
                .node_maker
                .make_node(vec![self.node_ref()], Consolidate { inner: self.inner }),
            context_id: self.context_id,
            depth: self.depth,
            phantom: PhantomData,
            node_maker: self.node_maker,
        }
    }
}
