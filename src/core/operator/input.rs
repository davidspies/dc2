use super::Op;
use crate::core::is_map::IsAddMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::{ContextId, CreationContext, ExecutionContext, Relation, Step};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;
use std::{cell::RefCell, ptr};

struct InputInner<D, R> {
    pending_step: usize,
    adding_step: usize,
    pending: HashMap<D, R>,
    adding: HashMap<D, R>,
}

trait IsInput {
    fn latest_update(&mut self, step: Step) -> Step;
}

impl<D: Key, R: Monoid> IsInput for InputInner<D, R> {
    fn latest_update(&mut self, step: Step) -> Step {
        self.resolve(step);
        self.pending_step
    }
}

#[derive(Clone)]
pub(in crate::core) struct InputRef(Rc<RefCell<dyn IsInput>>);
impl PartialEq for InputRef {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self.0.as_ptr(), other.0.as_ptr())
    }
}
impl Eq for InputRef {}
impl Hash for InputRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ptr::hash(self.0.as_ptr(), state)
    }
}

impl InputRef {
    pub(in crate::core) fn latest_update(&self, step: Step) -> Step {
        self.0.borrow_mut().latest_update(step)
    }
}

#[derive(Clone)]
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
impl<D: Key, R: Monoid> InputInner<D, R> {
    fn resolve(&mut self, step: usize) {
        assert!(self.adding_step <= step);
        if self.adding_step < step {
            if !self.adding.is_empty() {
                self.pending_step = self.adding_step;
                for (x, r) in mem::take(&mut self.adding) {
                    self.pending.add(x, r);
                }
            }
            self.adding_step = step;
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
            pending_step: 0,
            adding_step: 0,
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
                    .make_node(Vec::new(), InputCollection(Rc::clone(&inner))),
                context_id: self.context_id,
                phantom: PhantomData,
                node_maker: self.node_maker.clone(),
            }
            .with_input(InputRef(inner)),
        )
    }
}
