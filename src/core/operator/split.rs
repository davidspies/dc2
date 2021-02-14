use super::barrier::Barrier;
use super::Op;
use crate::core::is_map::IsAddMap;
use crate::core::iter::TupleableWith;
use crate::core::{Relation, Step};
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

struct Source<C: Op> {
    source: Barrier<C>,
    listeners: Vec<Rc<RefCell<HashMap<C::D, C::R>>>>,
}

pub struct Receiver<C: Op> {
    data: Rc<RefCell<HashMap<C::D, C::R>>>,
    source: Rc<RefCell<Source<C>>>,
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
        let mut source = self.source.borrow_mut();
        let Source {
            source: ref mut inner,
            ref listeners,
        } = &mut *source;
        inner.flow(step, |x, r| {
            for (listener, (x, r)) in listeners.iter().tuple_with((x, r)) {
                listener.borrow_mut().add(x, r);
            }
        });
        for (x, r) in mem::take(&mut *self.data.borrow_mut()) {
            send(x, r)
        }
    }
}

impl<'a, C: Op> Relation<'a, C> {
    pub fn split(self) -> Relation<'a, Receiver<C>> {
        let this = self.barrier();
        let data = Rc::new(RefCell::new(HashMap::new()));
        let source = Rc::new(RefCell::new(Source {
            source: this.inner,
            listeners: vec![Rc::clone(&data)],
        }));
        Relation {
            inner: Receiver { data, source },
            context_id: this.context_id,
            depth: this.depth,
            phantom: PhantomData,
        }
    }
}
