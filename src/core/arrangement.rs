use super::{ContextId, CreationContext, ExecutionContext, Relation, Step};
use crate::core::is_map::IsAddMap;
use crate::core::operator::{DynOp, Op};
use std::cell::{Ref, RefCell};
use std::collections::HashMap;

pub struct Arrangement<D, R, C = DynOp<D, R>> {
    inner: RefCell<ArrangementInner<D, R, C>>,
    context_id: ContextId,
}

impl<C: Op> Arrangement<C::D, C::R, C> {
    pub fn read<'a>(&'a self, context: &'a ExecutionContext) -> Ref<'a, HashMap<C::D, C::R>> {
        assert_eq!(self.context_id, context.context_id, "Context mismatch");
        if self.inner.borrow().step < context.step {
            self.inner.borrow_mut().flow(context.step);
        }
        Ref::map(self.inner.borrow(), |i| &i.value)
    }
}

struct ArrangementInner<D, R, C> {
    from: C,
    value: HashMap<D, R>,
    step: usize,
}

impl<C: Op> ArrangementInner<C::D, C::R, C> {
    fn flow<'a>(&'a mut self, step: usize) {
        let ArrangementInner {
            ref mut from,
            ref mut value,
            step: ref mut cur_step,
        } = self;
        *cur_step = step;
        from.flow(&Step::Root(step), |x, r| value.add(x, r));
    }
}

impl<C: Op> Relation<'static, C> {
    pub fn get_arrangement(self, context: &CreationContext) -> Arrangement<C::D, C::R, C> {
        assert_eq!(self.context_id, context.0, "Context mismatch");
        Arrangement {
            inner: RefCell::new(ArrangementInner {
                from: self.inner,
                step: 0,
                value: HashMap::new(),
            }),
            context_id: self.context_id,
        }
    }
}