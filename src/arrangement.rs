use super::{CWrapper, ContextId, CreationContext, ExecutionContext, Step};
use crate::is_map::IsAddMap;
use crate::operator::{Operator, WCollection};
use std::cell::{Ref, RefCell};
use std::collections::HashMap;

pub struct Arrangement<D, R, C = WCollection<D, R>> {
    inner: RefCell<ArrangementInner<D, R, C>>,
    context_id: ContextId,
}

impl<C: Operator> Arrangement<C::D, C::R, C> {
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
    step: Step,
}

impl<C: Operator> ArrangementInner<C::D, C::R, C> {
    fn flow<'a>(&'a mut self, step: Step) {
        let ArrangementInner {
            ref mut from,
            ref mut value,
            step: ref mut cur_step,
        } = self;
        *cur_step = step;
        from.flow(step, |x, r| value.add(x, r));
    }
}

impl<C: Operator> CWrapper<C> {
    pub fn get_c_arrangement(self, context: &CreationContext) -> Arrangement<C::D, C::R, C> {
        assert_eq!(self.context_id, context.0, "Context mismatch");
        Arrangement {
            inner: RefCell::new(ArrangementInner {
                from: self.inner,
                step: Step(0),
                value: HashMap::new(),
            }),
            context_id: self.context_id,
        }
    }
    pub fn get_arrangement(self, context: &CreationContext) -> Arrangement<C::D, C::R>
    where
        C: 'static,
    {
        self.wcollect().get_c_arrangement(context)
    }
}
