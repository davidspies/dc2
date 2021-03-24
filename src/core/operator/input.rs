use super::Op;
use crate::core::is_map::IsAddMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::{ContextId, CreationContext, ExecutionContext, Relation, Step};
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

struct InputInner<D, R> {
    step: usize,
    pending: HashMap<D, R>,
    adding: HashMap<D, R>,
}

pub struct Input<D, R = isize> {
    inner: Rc<RefCell<InputInner<D, R>>>,
    context_id: ContextId,
}
struct InputCollection<D, R>(Rc<RefCell<InputInner<D, R>>>);

impl<D: Key, R: Monoid> Input<D, R> {
    pub fn update(&self, context: &ExecutionContext, x: D, r: R) {
        assert_eq!(self.context_id, context.context_id);
        let mut inner_mut = self.inner.borrow_mut();
        inner_mut.resolve(context.step);
        inner_mut.adding.add(x, r);
    }
}
impl<D, R> Clone for Input<D, R> {
    fn clone(&self) -> Self {
        Input {
            inner: Rc::clone(&self.inner),
            context_id: self.context_id,
        }
    }
}
impl<D: Key, R: Monoid> InputInner<D, R> {
    fn resolve(&mut self, step: usize) {
        assert!(self.step <= step);
        if self.step < step {
            for (x, r) in mem::take(&mut self.adding) {
                self.pending.add(x, r);
            }
            self.step = step;
        }
    }
}
impl<D: Key, R: Monoid> InputInner<D, R> {
    fn flow<F: FnMut(D, R)>(&mut self, step: Step, mut send: F) {
        self.resolve(step);
        for (x, r) in mem::take(&mut self.pending) {
            send(x, r);
        }
    }
}

impl<D: Key, R: Monoid> Op for InputCollection<D, R> {
    type D = D;
    type R = R;

    fn default_op_name() -> &'static str {
        "input"
    }
    fn flow<F: FnMut(D, R)>(&mut self, step: Step, send: F) {
        self.0.borrow_mut().flow(step, send)
    }
}

impl CreationContext {
    pub fn create_input<D: Key, R: Monoid>(
        &self,
    ) -> (Input<D, R>, Relation<'static, impl Op<D = D, R = R>>) {
        let inner = Rc::new(RefCell::new(InputInner {
            step: 0,
            pending: HashMap::new(),
            adding: HashMap::new(),
        }));
        (
            Input {
                inner: Rc::clone(&inner),
                context_id: self.context_id,
            },
            Relation {
                inner: self
                    .node_maker
                    .make_node(Vec::new(), InputCollection(inner)),
                context_id: self.context_id,
                phantom: PhantomData,
                node_maker: self.node_maker.clone(),
            },
        )
    }
}
