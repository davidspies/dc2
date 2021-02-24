use crate::core::is_map::HybridMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::operator::Op;
use crate::core::Step;
use std::cell::RefCell;
use std::rc::Rc;

pub(super) struct SimpleInput<D, R>(pub(super) Rc<RefCell<HybridMap<D, R>>>);

impl<D: Key, R: Monoid> Op for SimpleInput<D, R> {
    type D = D;
    type R = R;
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, _step: &Step, mut send: F) {
        for (x, r) in self.0.borrow_mut().steal() {
            send(x, r)
        }
    }
}
