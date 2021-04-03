use super::{ContextId, CreationContext, ExecutionContext, Relation};
use crate::core::is_map::IsAddMap;
use crate::core::node::{Node, NodeInfo};
use crate::core::operator::{DynOp, InputRef, Op};
use std::cell::{Ref, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub struct Arrangement<
    D,
    R = isize,
    M: IsAddMap<D, R> = HashMap<D, R>,
    C: Op<D = D, R = R> = DynOp<D, R>,
> {
    inner: Rc<RefCell<ArrangementInner<D, R, M, C>>>,
    context_id: ContextId,
}

impl<M: IsAddMap<C::D, C::R>, C: Op> Clone for Arrangement<C::D, C::R, M, C> {
    fn clone(&self) -> Self {
        Arrangement {
            inner: Rc::clone(&self.inner),
            context_id: self.context_id,
        }
    }
}

impl<C: Op, M: IsAddMap<C::D, C::R>> Arrangement<C::D, C::R, M, C> {
    pub fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, M> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        self.inner.borrow_mut().flow(context.step);
        Ref::map(self.inner.borrow(), |i| &i.value)
    }
    pub fn get_inputs(&self) -> Inputs {
        Inputs(Rc::clone(&self.inner.borrow().from.info))
    }
}

pub struct Inputs(Rc<RefCell<NodeInfo>>);

impl Inputs {
    pub fn borrow(&self) -> Ref<HashSet<InputRef>> {
        Ref::map(self.0.borrow(), |x| &x.inputs)
    }
}

struct ArrangementInner<D, R, M: IsAddMap<D, R>, C: Op<D = D, R = R>> {
    from: Node<C>,
    value: M,
    step: usize,
}

impl<C: Op, M: IsAddMap<C::D, C::R>> ArrangementInner<C::D, C::R, M, C> {
    fn flow(&mut self, step: usize) {
        let ArrangementInner {
            ref mut from,
            ref mut value,
            step: ref mut cur_step,
        } = self;
        if from.needs_update(*cur_step, step) {
            *cur_step = step;
            from.flow(step, |x, r| value.add(x, r));
        }
    }
}

impl<C: Op> Relation<C> {
    pub fn get_arrangement<M: IsAddMap<C::D, C::R>>(
        self,
        context: &CreationContext,
    ) -> Arrangement<C::D, C::R, M, C> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        Arrangement {
            inner: Rc::new(RefCell::new(ArrangementInner {
                from: self.inner,
                step: 0,
                value: Default::default(),
            })),
            context_id: self.context_id,
        }
    }
}
