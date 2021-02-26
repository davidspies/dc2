use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::operator::Op;
use crate::core::Step;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::rc::Rc;

pub(super) struct SimpleInput<D, R>(pub(super) Rc<RefCell<HashMap<D, R>>>);

impl<D: Key, R: Monoid> Op for SimpleInput<D, R> {
    type D = D;
    type R = R;
    fn hideable() -> bool {
        false
    }
    fn default_op_name() -> &'static str {
        "variable"
    }
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, _step: &Step, mut send: F) {
        for (x, r) in mem::take(&mut *self.0.borrow_mut()) {
            send(x, r)
        }
    }
}
