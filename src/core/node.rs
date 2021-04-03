use crate::core::{
    operator::{InputRef, Op},
    Relation, Step,
};
use std::collections::HashSet;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub(super) struct Node<C: ?Sized> {
    pub(super) info: Rc<RefCell<NodeInfo>>,
    pub(super) inner: C,
}

impl<C: Op> Node<C> {
    pub(super) fn set_name(&mut self, name: String) {
        self.info.borrow_mut().set_name(name)
    }
    pub(super) fn set_op_name(&mut self, name: String) {
        self.info.borrow_mut().set_op_name(name)
    }
    pub(super) fn needs_update(&self, prev_step: Step, step: Step) -> bool {
        self.info
            .borrow()
            .inputs
            .iter()
            .any(|inp| prev_step <= inp.latest_update(step))
    }
    pub(super) fn flow<F: FnMut(C::D, C::R)>(&mut self, step: Step, mut send: F) {
        let Node { inner, info } = self;
        inner.flow(step, |x, r| {
            send(x, r);
            info.borrow_mut().message_count += 1;
        })
    }
}

type RelationId = usize;

pub(super) struct NodeInfo {
    pub(super) name: Option<String>,
    pub(super) operator_name: String,
    pub(super) shown: bool,
    pub(super) message_count: usize,
    pub(super) relation_id: RelationId,
    pub(super) deps: Vec<Rc<RefCell<NodeInfo>>>,
    pub(super) hideable: bool,
    pub(super) inputs: HashSet<InputRef>,
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
            self.deps[0].borrow_mut().apply_to_shown(f)
        }
    }
    pub(super) fn shown_relation_id(&self) -> RelationId {
        if self.shown {
            self.relation_id
        } else {
            assert_eq!(self.deps.len(), 1);
            self.deps[0].borrow().shown_relation_id()
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
    pub(super) fn make_node<C: Op>(&self, deps: Vec<Rc<RefCell<NodeInfo>>>, inner: C) -> Node<C> {
        let mut infos = self.infos.borrow_mut();
        let inputs = deps
            .iter()
            .flat_map(|inf| inf.borrow().inputs.clone())
            .collect();
        let info = Rc::new(RefCell::new(NodeInfo {
            message_count: 0,
            name: None,
            shown: true,
            operator_name: C::default_op_name().to_string(),
            relation_id: infos.len(),
            deps,
            hideable: C::hideable(),
            inputs,
        }));
        infos.push(Rc::clone(&info));
        Node { inner, info }
    }
}

impl<C: ?Sized> Relation<C> {
    pub(super) fn node_ref(&self) -> &Rc<RefCell<NodeInfo>> {
        &self.inner.info
    }
}
