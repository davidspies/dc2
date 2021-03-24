use super::barrier::Barrier;
use super::Op;
use crate::core::is_map::IsAddMap;
use crate::core::iter::TupleableWith;
use crate::core::node::Node;
use crate::core::{Relation, Step};
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::mem;
use std::rc::Rc;

struct Source<C: Op> {
    inner: Barrier<C>,
    listeners: Vec<Rc<RefCell<HashMap<C::D, C::R>>>>,
}

pub(super) struct SourceRef<C: Op>(Rc<RefCell<Source<C>>>);

impl<C: Op> SourceRef<C> {
    pub(super) fn get_inner(&self) -> Ref<Node<C>> {
        Ref::map(self.0.borrow(), |r| &r.inner.inner)
    }
    pub(super) fn propagate(&self, step: Step) {
        if self.0.borrow().inner.dirty(step) {
            let mut source = self.0.borrow_mut();
            let Source {
                ref mut inner,
                ref listeners,
            } = &mut *source;
            inner.flow(step, |x, r| {
                for (listener, (x, r)) in listeners.iter().tuple_with((x, r)) {
                    listener.borrow_mut().add(x, r);
                }
            });
        }
    }
    fn add_listener(&self, listener: Rc<RefCell<HashMap<C::D, C::R>>>) {
        self.0.borrow_mut().listeners.push(listener)
    }
}

pub struct Receiver<C: Op> {
    data: Rc<RefCell<HashMap<C::D, C::R>>>,
    source: SourceRef<C>,
}

impl<C: Op> Receiver<C> {
    pub(super) fn new(from: Node<C>) -> Self {
        let inner = Barrier::new(from);
        let data = Rc::new(RefCell::new(HashMap::new()));
        let source = SourceRef(Rc::new(RefCell::new(Source {
            inner,
            listeners: vec![Rc::clone(&data)],
        })));
        Receiver { data, source }
    }
    pub(super) fn get_source_ref(&self) -> SourceRef<C> {
        self.source.clone()
    }
}

impl<C: Op> Clone for SourceRef<C> {
    fn clone(&self) -> Self {
        SourceRef(Rc::clone(&self.0))
    }
}

impl<C: Op> Clone for Receiver<C> {
    fn clone(&self) -> Self {
        let data = Rc::new(RefCell::new(self.data.borrow().clone()));
        self.source.add_listener(Rc::clone(&data));
        Receiver {
            data,
            source: self.source.clone(),
        }
    }
}

impl<C: Op> Op for Receiver<C> {
    type D = C::D;
    type R = C::R;

    fn default_op_name() -> &'static str {
        "split"
    }
    fn flow<F: FnMut(C::D, C::R)>(&mut self, step: Step, mut send: F) {
        self.source.propagate(step);
        for (x, r) in mem::take(&mut *self.data.borrow_mut()) {
            send(x, r)
        }
    }
}

impl<'a, C: Op> Relation<'a, C> {
    /// Produces a version of this relation which can be cloned to use in multiple places.
    pub fn split(self) -> Relation<'a, Receiver<C>> {
        Relation::new(vec![self.dep()], Receiver::new(self.inner)).hidden()
    }
}
