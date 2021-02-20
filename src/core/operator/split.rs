use super::barrier::Barrier;
use super::Op;
use crate::core::is_map::IsAddMap;
use crate::core::iter::TupleableWith;
use crate::core::{Relation, Step};
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

struct Source<C: Op> {
    inner: Barrier<C>,
    listeners: Vec<Rc<RefCell<HashMap<C::D, C::R>>>>,
}

pub struct Receiver<C: Op> {
    data: Rc<RefCell<HashMap<C::D, C::R>>>,
    source: Rc<RefCell<Source<C>>>,
}

impl<C: Op> Receiver<C> {
    pub(super) fn new(from: C, depth: usize) -> Self {
        let inner = Barrier::new(from, depth);
        let data = Rc::new(RefCell::new(HashMap::new()));
        let source = Rc::new(RefCell::new(Source {
            inner,
            listeners: vec![Rc::clone(&data)],
        }));
        Receiver { data, source }
    }
    pub(super) fn get_inner(&self) -> Ref<C> {
        Ref::map(self.source.borrow(), |r| &r.inner.inner)
    }
    pub(super) fn get_inner_mut(&self) -> RefMut<C> {
        RefMut::map(self.source.borrow_mut(), |r| &mut r.inner.inner)
    }
    pub(super) fn propagate(&self, step: &Step) {
        if self.source.borrow().inner.dirty(step) {
            let mut source = self.source.borrow_mut();
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
}

impl<C: Op> Clone for Receiver<C> {
    fn clone(&self) -> Self {
        let data = Rc::new(RefCell::new(self.data.borrow().clone()));
        self.source.borrow_mut().listeners.push(Rc::clone(&data));
        Receiver {
            data,
            source: self.source.clone(),
        }
    }
}

impl<C: Op> Op for Receiver<C> {
    type D = C::D;
    type R = C::R;

    fn flow<F: FnMut(C::D, C::R)>(&mut self, step: &Step, mut send: F) {
        self.propagate(step);
        for (x, r) in mem::take(&mut *self.data.borrow_mut()) {
            send(x, r)
        }
    }
}

impl<'a, C: Op> Relation<'a, C> {
    pub fn split(self) -> Relation<'a, Receiver<C>> {
        Relation {
            inner: Receiver::new(self.inner, self.depth),
            context_id: self.context_id,
            depth: self.depth,
            phantom: PhantomData,
        }
    }
}
