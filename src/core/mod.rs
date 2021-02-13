mod arrangement;
pub mod borrow;
pub mod emptyable;
pub mod is_map;
mod iter;
pub mod key;
pub mod monoid;
mod operator;

pub use self::arrangement::Arrangement;
pub use self::operator::{Collection, DynOp, Input, Op, Operator};
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
    step: Step,
    context_id: ContextId,
}

impl CreationContext {
    pub fn begin(self) -> ExecutionContext {
        ExecutionContext {
            step: Step(0),
            context_id: self.0,
        }
    }
}

impl ExecutionContext {
    pub fn commit(&mut self) {
        self.step.0 += 1;
    }
}

#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub struct Step(usize);

#[derive(Clone)]
pub struct Relation<C> {
    inner: C,
    context_id: ContextId,
}
