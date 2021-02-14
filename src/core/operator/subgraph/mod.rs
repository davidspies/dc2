mod contextual;
mod leave;
mod registrar;
mod simple_input;
mod stepper;
mod variable;

use self::contextual::IsContext;
use self::registrar::Registrar;
pub use self::variable::Variable;
use crate::core::key::Key;
use crate::core::{ContextId, CreationContext};

impl<'a, Ctx: IsContext, S: Key + Ord> IsContext for SubContext<'a, Ctx, S> {
    fn get_context_id(&self) -> ContextId {
        self.context_id
    }
    fn get_depth() -> usize {
        Ctx::get_depth() + 1
    }
}
pub struct SubContext<'a, Ctx, S: Key + Ord> {
    parent: &'a Ctx,
    registrar: Registrar<S>,
    context_id: ContextId,
}
pub struct Finalizer<'a, Ctx, S: Key + Ord> {
    parent: &'a Ctx,
    registrar: Registrar<S>,
}

impl CreationContext {
    pub fn subgraph<'a, S: Key + Ord>(&'a mut self) -> SubContext<'a, Self, S> {
        SubContext::from(self)
    }
}

impl<'a, Ctx: IsContext, S: Key + Ord> SubContext<'a, Ctx, S> {
    fn from(parent: &'a Ctx) -> Self {
        SubContext {
            parent,
            registrar: Registrar::new_registrar(Ctx::get_depth()),
            context_id: parent.get_context_id(),
        }
    }
    pub fn subgraph<'b, T: Key + Ord>(&'b mut self) -> SubContext<'b, Self, T> {
        SubContext::from(self)
    }
    pub fn finish(self) -> Finalizer<'a, Ctx, S> {
        Finalizer {
            parent: self.parent,
            registrar: self.registrar,
        }
    }
}
