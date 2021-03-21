use crate::core::{operator::Op, InputTrigger, Relation, Step};
use std::{
    cell::RefCell,
    collections::HashSet,
    rc::{Rc, Weak},
};

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
    pub(super) fn set_trigger(self, t: InputTrigger) -> Self {
        self.info.borrow_mut().triggers = TriggerState::Visited(vec![t].into_iter().collect());
        self
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
    triggers: TriggerState,
}

enum TriggerState {
    Unvisited,
    Searching(usize),
    Visited(HashSet<InputTrigger>),
}

impl TriggerState {
    fn is_unvisited(&self) -> bool {
        match self {
            Self::Unvisited => true,
            _ => false,
        }
    }
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
    pub(super) fn determine_inputs(this: Rc<RefCell<Self>>) -> HashSet<InputTrigger> {
        let (d, equ, res) = Self::determine_inputs_helper(this, 0);
        assert_eq!(d, 0);
        assert!(equ.is_empty());
        res
    }
    fn determine_inputs_helper(
        this: Rc<RefCell<Self>>,
        depth: usize,
    ) -> (usize, Vec<Rc<RefCell<NodeInfo>>>, HashSet<InputTrigger>) {
        if this.borrow().triggers.is_unvisited() {
            this.borrow_mut().triggers = TriggerState::Searching(depth);
            let mut min_depth = depth + 1;
            let mut equivalent = Vec::new();
            let mut res = HashSet::new();
            for dep in this.borrow().deps.iter() {
                let (cycle_depth, dep_equivalent, x) =
                    Self::determine_inputs_helper(dep.upgrade().unwrap(), depth + 1);
                min_depth = min_depth.min(cycle_depth);
                equivalent.extend(dep_equivalent);
                res.extend(x);
            }
            if min_depth < depth {
                this.borrow_mut().triggers = TriggerState::Searching(min_depth);
                equivalent.push(this);
                (min_depth, equivalent, res)
            } else {
                this.borrow_mut().triggers = TriggerState::Visited(res.clone());
                for equ in equivalent {
                    equ.borrow_mut().triggers = TriggerState::Visited(res.clone());
                }
                (depth, Vec::new(), res)
            }
        } else {
            match &this.borrow().triggers {
                &TriggerState::Unvisited => panic!("Unreachable"),
                &TriggerState::Searching(d) => (d, Vec::new(), HashSet::new()),
                TriggerState::Visited(x) => (depth, Vec::new(), x.clone()),
            }
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
            triggers: TriggerState::Unvisited,
        }));
        infos.push(Rc::clone(&info));
        Node { inner, info }
    }
}

impl<C: ?Sized> Relation<'_, C> {
    pub(super) fn node_ref(&self) -> Weak<RefCell<NodeInfo>> {
        Rc::downgrade(&self.inner.info)
    }
}
