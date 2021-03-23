use super::contextual::IsContext;
use super::simple_input::SimpleInput;
use super::stepper::Stepper;
use super::{Registrar, SubContext};
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::node::NodeInfo;
use crate::core::operator::Op;
use crate::core::{ContextId, Relation};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::rc::Rc;

pub struct Variable<'a, S: Key + Ord, D, R> {
    inner: Rc<RefCell<HashMap<(S, D), R>>>,
    context_id: ContextId,
    registrar: Registrar<S>,
    phantom: PhantomData<&'a ()>,
    info: Rc<RefCell<NodeInfo>>,
}

impl<'a, S: Key + Ord, D: Key, R: Monoid> Variable<'a, S, D, R> {
    pub fn set<C: Op<D = (S, D), R = R>>(mut self, rel: Relation<'a, C>) {
        assert_eq!(self.context_id, rel.context_id, "Context mismatch");
        self.info.borrow_mut().deps.push(rel.node_ref());
        self.registrar
            .add_stepper(Stepper::new(BTreeMap::new(), self.inner, rel.inner))
    }
}

impl<'a, Ctx: IsContext, S: Key + Ord> SubContext<'a, Ctx, S> {
    pub fn variable<'b, D: Key, R: Monoid>(
        &'b mut self,
    ) -> (
        Variable<'a, S, D, R>,
        Relation<'a, impl Op<D = (S, D), R = R>>,
    ) {
        let pending = Rc::new(RefCell::new(HashMap::new()));
        let depth = Self::get_depth();
        let new_node = self
            .node_maker
            .make_node(
                vec![Rc::downgrade(&self.registrar.get_inner().info)],
                SimpleInput {
                    pending: Rc::clone(&pending),
                },
            )
            .with_depth(depth);
        (
            Variable {
                inner: pending,
                context_id: self.context_id,
                registrar: self.registrar.clone(),
                phantom: PhantomData,
                info: new_node.info.clone(),
            },
            Relation {
                inner: new_node,
                context_id: self.context_id,
                phantom: PhantomData,
                node_maker: self.node_maker.clone(),
            },
        )
    }
}
