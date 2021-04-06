use super::{ContextId, CreationContext, ExecutionContext, Relation, Step};
use crate::core::is_map::IsAddMap;
use crate::core::node::Node;
use crate::core::operator::{DynOp, Op};
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

pub struct ArrangementG<C: Op, M = HashMap<<C as Op>::D, <C as Op>::R>> {
    inner: Rc<RefCell<ArrangementInner<C, M>>>,
    context_id: ContextId,
}

pub type Arrangement<D, R = isize, M = HashMap<D, R>> = ArrangementG<DynOp<D, R>, M>;

impl<M: IsAddMap<C::D, C::R>, C: Op> Clone for ArrangementG<C, M> {
    fn clone(&self) -> Self {
        ArrangementG {
            inner: Rc::clone(&self.inner),
            context_id: self.context_id,
        }
    }
}

impl<C: Op, M: IsAddMap<C::D, C::R>> ArrangementG<C, M> {
    pub fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, M> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        self.inner.borrow_mut().flow(context.step);
        Ref::map(self.inner.borrow(), |i| &i.value)
    }
}

struct ArrangementInner<C: Op, M> {
    from: Node<C>,
    value: M,
    step: Step,
}

impl<C: Op, M: IsAddMap<C::D, C::R>> ArrangementInner<C, M> {
    fn flow(&mut self, step: Step) {
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
    ) -> ArrangementG<C, M> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        ArrangementG {
            inner: Rc::new(RefCell::new(ArrangementInner {
                from: self.inner,
                step: 0,
                value: Default::default(),
            })),
            context_id: self.context_id,
        }
    }
}
