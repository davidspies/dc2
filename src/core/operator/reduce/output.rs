use super::IsReduce;
use crate::core::node::Node;
use crate::core::operator::barrier::Barrier;
use crate::core::operator::split::{Receiver, SourceRef};
use crate::core::operator::Op;
use crate::core::{
    context::{ContextId, CreationContext, ExecutionContext},
    Relation,
};
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::ops::Deref;

impl<C: IsReduce + Op> Relation<C> {
    pub fn split_reduce_output(
        self,
        context: &CreationContext,
    ) -> (Relation<Receiver<C>>, impl ReduceOutput<K = C::K, M = C::M>) {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        let context_id = self.context_id;
        let r = self.split();
        let inner = r.inner.inner.get_source_ref();
        (r, SplitReduceOutputImpl { context_id, inner })
    }
    pub fn reduce_output(self, context: &CreationContext) -> impl ReduceOutput<K = C::K, M = C::M> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        let context_id = self.context_id;
        let r = self.barrier();
        ReduceOutputImpl {
            context_id,
            inner: RefCell::new(r.inner),
        }
    }
}

pub struct SplitReduceOutputImpl<C: Op> {
    context_id: ContextId,
    inner: SourceRef<C>,
}

pub struct ReduceOutputImpl<C: Op> {
    context_id: ContextId,
    inner: RefCell<Node<Barrier<C>>>,
}

impl<C: IsReduce + Op> ReduceOutput for SplitReduceOutputImpl<C> {
    type K = C::K;
    type M = C::M;

    fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<C::K, C::M>> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        self.inner.propagate(context.step);
        Ref::map(self.inner.get_inner(), |n| IsReduce::get_ref(&n.inner))
    }
}

impl<C: IsReduce + Op> ReduceOutput for ReduceOutputImpl<C> {
    type K = C::K;
    type M = C::M;

    fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<C::K, C::M>> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        if self.inner.borrow().inner.dirty(context.step) {
            self.inner.borrow_mut().flow(context.step, |_, _| ());
        }
        Ref::map(self.inner.borrow(), |n| n.inner.inner.inner.get_ref())
    }
}

pub trait ReduceOutput {
    type K;
    type M;
    fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<Self::K, Self::M>>;
}

impl<T: ReduceOutput> ReduceOutput for Box<T> {
    type K = T::K;
    type M = T::M;
    fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<T::K, T::M>> {
        <Box<T> as Deref>::deref(self).read(context)
    }
}
