use super::Relation;
use crate::core::operator::Op;
use crate::core::Step;
use std::cell::RefCell;
use std::rc::{Rc, Weak};

#[derive(Clone)]
pub(super) struct Node<C> {
    pub(super) inner: C,
    pub(super) info: Rc<RefCell<NodeInfo>>,
}

impl<C: Op> Node<C> {
    pub(super) fn set_name(&mut self, name: String) {
        self.info.borrow_mut().set_name(name)
    }
    pub(super) fn set_op_name(&mut self, name: String) {
        self.info.borrow_mut().set_op_name(name)
    }
    pub(super) fn flow<F: FnMut(C::D, C::R)>(&mut self, step: &Step, mut send: F) {
        let Node { inner, info } = self;
        inner.flow(step, |x, r| {
            send(x, r);
            info.borrow_mut().message_count += 1;
        })
    }
    pub(super) fn node_ref(&self) -> Weak<RefCell<NodeInfo>> {
        Rc::downgrade(&self.info)
    }
}

type RelationId = usize;

pub(super) struct NodeInfo {
    pub(super) name: Option<String>,
    pub(super) operator_name: String,
    pub(super) shown: bool,
    pub(super) message_count: usize,
    pub(super) relation_id: RelationId,
    pub(super) deps: Vec<Weak<RefCell<NodeInfo>>>,
    pub(super) hideable: bool,
}

impl NodeInfo {
    pub(super) fn set_name(&mut self, name: String) {
        self.apply_to_shown(|n| n.name = Some(name))
    }
    pub(super) fn set_op_name(&mut self, name: String) {
        self.apply_to_shown(|n| n.operator_name = name)
    }
    fn apply_to_shown<F: FnOnce(&mut Self)>(&mut self, f: F) {
        if self.shown {
            f(self)
        } else {
            assert_eq!(self.deps.len(), 1);
            self.deps[0]
                .upgrade()
                .unwrap()
                .borrow_mut()
                .apply_to_shown(f)
        }
    }
    pub(super) fn shown_relation_id(&self) -> RelationId {
        if self.shown {
            self.relation_id
        } else {
            assert_eq!(self.deps.len(), 1);
            self.deps[0].upgrade().unwrap().borrow().shown_relation_id()
        }
    }
}

#[derive(Clone)]
pub struct NodeMaker {
    pub(super) infos: Rc<RefCell<Vec<Rc<RefCell<NodeInfo>>>>>,
}

impl NodeMaker {
    pub(super) fn new() -> Self {
        NodeMaker {
            infos: Rc::new(RefCell::new(Vec::new())),
        }
    }
    pub(super) fn make_node<C: Op>(&self, deps: Vec<Weak<RefCell<NodeInfo>>>, inner: C) -> Node<C> {
        let mut infos = self.infos.borrow_mut();
        let info = Rc::new(RefCell::new(NodeInfo {
            message_count: 0,
            name: None,
            shown: true,
            operator_name: C::default_op_name().to_string(),
            relation_id: infos.len(),
            deps,
            hideable: C::hideable(),
        }));
        infos.push(Rc::clone(&info));
        Node { inner, info }
    }
}

impl<C: Op> Relation<'_, C> {
    pub(super) fn node_ref(&self) -> Weak<RefCell<NodeInfo>> {
        Rc::downgrade(&self.inner.info)
    }
}
