mod arrangement;
pub mod borrow;
pub mod emptyable;
pub mod is_map;
pub mod iter;
pub mod key;
pub mod monoid;
mod operator;

pub use self::arrangement::Arrangement;
pub use self::operator::{DynOp, Input, Op, Receiver};
use std::marker::PhantomData;
use std::sync::atomic::{self, AtomicUsize};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn next_id() -> ContextId {
    NEXT_ID.fetch_add(1, atomic::Ordering::SeqCst)
}

type ContextId = usize;

pub struct CreationContext(ContextId);

impl CreationContext {
    pub fn new() -> Self {
        CreationContext(next_id())
    }
}

pub struct ExecutionContext {
    step: usize,
    context_id: ContextId,
}

impl CreationContext {
    pub fn begin(self) -> ExecutionContext {
        ExecutionContext {
            step: 0,
            context_id: self.0,
        }
    }
}

impl ExecutionContext {
    pub fn commit(&mut self) {
        self.step += 1;
    }
}

pub struct Sub<'a> {
    depth: usize,
    step: usize,
    parent: &'a Step<'a>,
}

pub enum Step<'a> {
    Root(usize),
    Sub(Sub<'a>),
}

impl<'a> Step<'a> {
    fn get_last(&self) -> usize {
        match self {
            &Step::Root(s) => s,
            &Step::Sub(Sub { step, .. }) => step,
        }
    }
    fn step_for(&self, depth: usize) -> &Step {
        match self {
            &Self::Root(_) => self,
            &Self::Sub(Sub {
                depth: my_depth,
                step: _,
                ref parent,
            }) => {
                if my_depth == depth {
                    self
                } else {
                    assert!(depth < my_depth);
                    parent.step_for(depth)
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Relation<'a, C> {
    inner: C,
    context_id: ContextId,
    depth: usize,
    phantom: PhantomData<&'a ()>,
}