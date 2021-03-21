use super::Op;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::node::Node;
use crate::core::Relation;
use crate::core::Step;

pub struct DynOp<D, R = isize>(Box<dyn DynOpT<D = D, R = R>>);

trait DynOpT: 'static {
    type D: Key;
    type R: Monoid;
    fn flow_dyn(&mut self, step: &Step, send: &mut dyn FnMut(Self::D, Self::R));
}

impl<T: Op> DynOpT for Node<T> {
    type D = <T as Op>::D;
    type R = <T as Op>::R;
    fn flow_dyn(&mut self, step: &Step, send: &mut dyn FnMut(Self::D, Self::R)) {
        self.flow(step, send)
    }
}

impl<D: Key, R: Monoid> Op for DynOp<D, R> {
    type D = D;
    type R = R;
    fn default_op_name() -> &'static str {
        "dynamic"
    }
    fn flow<F: FnMut(D, R)>(&mut self, step: &Step, mut send: F) {
        self.0.flow_dyn(step, &mut send)
    }
}

impl<'a, C: Op> Relation<'a, C> {
    /// Throws out the implementation details in the template parameter, simplifying the
    /// type-signature at a cost of having to look them up at run-time.
    pub fn dynamic(self) -> Relation<'a, DynOp<C::D, C::R>> {
        Relation::new(
            vec![self.dep()],
            DynOp(Box::new(self.inner)),
            self.node_maker,
        )
        .hidden()
    }
}
