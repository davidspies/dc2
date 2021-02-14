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
    source: C,
    listeners: Vec<Rc<RefCell<HashMap<C::D, C::R>>>>,
    step: Step,
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

    fn flow<F: FnMut(C::D, C::R)>(&mut self, step: Step, mut send: F) {
        if self.source.borrow().step < step {
            let mut source = self.source.borrow_mut();
            let Source {
                source: ref mut inner,
                ref listeners,
                step: ref mut prev_step,
            } = &mut *source;
            *prev_step = step;
            let mut changes = HashMap::new();
            inner.flow(step, |x, r| changes.add(x, r));
            for (listener, changes) in listeners.iter().tuple_with(changes) {
                let mut lborrowed = listener.borrow_mut();
                for (x, r) in changes {
                    lborrowed.add(x, r);
                }
            }
        }
        for (x, r) in mem::take(&mut *self.data.borrow_mut()) {
            send(x, r)
        }
    }
}

impl<'a, C: Op> Relation<'a, C> {
    pub fn split(self) -> Relation<'a, Receiver<C>> {
        let data = Rc::new(RefCell::new(HashMap::new()));
        let source = Rc::new(RefCell::new(Source {
            source: self.inner,
            listeners: vec![Rc::clone(&data)],
            step: Step(0),
        }));
        Relation {
            inner: Receiver { data, source },
            context_id: self.context_id,
            depth: self.depth,
            phantom: PhantomData,
        }
    }
}
