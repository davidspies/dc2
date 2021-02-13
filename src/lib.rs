mod arrangement;
mod borrow;
mod emptyable;
mod is_map;
mod iter;
mod key;
mod monoid;
mod operator;

pub use self::arrangement::Arrangement;
pub use self::operator::{Collection, DynOperator, Operator, WCollection};

type ContextId = usize;

pub struct CreationContext(ContextId);

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
pub struct CWrapper<C> {
    inner: C,
    context_id: ContextId,
}

#[cfg(test)]
mod tests;
