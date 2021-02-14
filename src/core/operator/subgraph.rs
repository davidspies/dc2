use super::Op;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::{ContextId, CreationContext, Relation};
use std::marker::PhantomData;

pub struct SubContext<'a, Ctx, S>(&'a Ctx, PhantomData<S>);
pub struct Finalizer<'a, Ctx>(&'a Ctx);

impl CreationContext {
    pub fn subgraph<'a, S>(&'a mut self) -> SubContext<'a, CreationContext, S> {
        SubContext(self, PhantomData)
    }
}

pub struct Variable<'a, S, D, R> {
    inner: PhantomData<&'a (S, D, R)>,
    context_id: ContextId,
}

impl<'a, Ctx, S: Key> SubContext<'a, Ctx, S> {
    pub fn variable<D: Key, R: Monoid>(&self) -> (Variable<'a, S, D, R>, ()) {
        // Relation<impl Op<D = (S, D), R = R>>
        (unimplemented!(), unimplemented!())
    }
    pub fn finish(self) -> Finalizer<'a, Ctx> {
        Finalizer(self.0)
    }
}

impl<'a, S, D, R> Variable<'a, S, D, R> {
    pub fn set<C: Op<D = (S, D), R = R>>(self, rel: Relation<'a, C>) {
        assert_eq!(self.context_id, rel.context_id, "Context mismatch");
        unimplemented!()
    }
}

impl<'a, C> Relation<'a, C> {
    pub fn leave<'b, Ctx>(self, finalizer: Finalizer<'a, Ctx>) -> Relation<'b, C>
    where
        'b: 'a,
        Ctx: 'b,
    {
        // Relation<'b, impl Op<D=C::D, R=C::R>>
        // Assert contexts match
        unimplemented!()
    }
}
