use super::{
    context::ContextId,
    node::{Node, NodeInfo, NodeMaker},
    operator::{InputRef, Op},
};
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub struct Relation<C: ?Sized> {
    pub(super) context_id: ContextId,
    pub(super) node_maker: NodeMaker,
    pub(super) inner: Node<C>,
}

pub(super) struct Dep {
    context_id: ContextId,
    node_info: Rc<RefCell<NodeInfo>>,
    node_maker: NodeMaker,
}

impl<C: Op> Relation<C> {
    pub(super) fn new(deps: Vec<Dep>, inner: C) -> Self {
        let context_id = deps[0].context_id;
        for dep in &deps[1..] {
            assert_eq!(dep.context_id, context_id, "Context mismatch")
        }
        let node_maker = deps[0].node_maker.clone();
        Relation {
            inner: node_maker.make_node(deps.into_iter().map(|x| x.node_info).collect(), inner),
            context_id,
            node_maker,
        }
    }
    pub(super) fn dep(&self) -> Dep {
        Dep {
            context_id: self.context_id,
            node_info: Rc::clone(self.node_ref()),
            node_maker: self.node_maker.clone(),
        }
    }
    pub fn named(mut self, name: &str) -> Self {
        self.inner.set_name(name.to_string());
        self
    }
    pub fn op_named(mut self, name: &str) -> Self {
        self.inner.set_op_name(name.to_string());
        self
    }
    pub fn hidden(self) -> Self {
        assert!(
            self.inner.info.borrow().hideable,
            "Unhideable relation type"
        );
        assert_eq!(
            self.inner.info.borrow().deps.len(),
            1,
            "Too many inputs to hide"
        );
        self.set_shown(false)
    }
    pub fn shown(self) -> Self {
        self.set_shown(true)
    }
    fn set_shown(self, shown: bool) -> Self {
        self.inner.info.borrow_mut().shown = shown;
        self
    }
    pub(super) fn with_input(self, input: InputRef) -> Self {
        self.inner.info.borrow_mut().inputs.insert(input);
        self
    }
}
