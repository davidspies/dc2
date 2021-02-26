use crate::core::operator::Op;
use crate::core::Step;
use std::cell::Cell;
use std::rc::Rc;

#[derive(Clone)]
pub(super) struct Node<C> {
    pub(super) inner: C,
    name: Option<String>,
    message_count: usize,
    relation_id: usize,
}

impl<C> Node<C> {
    pub(super) fn set_name(&mut self, name: String) {
        self.name = Some(name)
    }
}

impl<C: Op> Node<C> {
    pub(super) fn flow<F: FnMut(C::D, C::R)>(&mut self, step: &Step, mut send: F) {
        let Node {
            inner,
            name: _,
            message_count,
            relation_id: _,
        } = self;
        inner.flow(step, |x, r| {
            send(x, r);
            *message_count += 1;
        })
    }
}

#[derive(Clone)]
pub struct NodeMaker {
    counter: Rc<Cell<usize>>,
}

impl NodeMaker {
    pub(super) fn new() -> Self {
        NodeMaker {
            counter: Rc::new(Cell::new(0)),
        }
    }
    pub(super) fn next_rel_id(&self) -> usize {
        let id = self.counter.get();
        self.counter.set(id + 1);
        id
    }
    pub(super) fn make_node<C>(&self, inner: C) -> Node<C> {
        Node {
            inner,
            message_count: 0,
            name: None,
            relation_id: self.next_rel_id(),
        }
    }
}
