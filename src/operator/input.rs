use super::{default_flow_to, DynOperator, Operator};
use crate::key::Key;
use crate::monoid::Monoid;
use crate::Step;
use std::collections::HashMap;
use std::mem;

struct Input<D, R> {
    current_step: Step,
    pending: HashMap<D, R>,
}

impl<D: Key, R: Monoid> DynOperator for Input<D, R> {
    type D = D;
    type R = R;
    fn flow_to(&mut self, step: Step) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}

impl<D: Key, R: Monoid> Operator for Input<D, R> {
    fn flow<F: FnMut(D, R)>(&mut self, step: Step, mut send: F) {
        let xs = if step > self.current_step {
            mem::take(&mut self.pending)
        } else {
            return;
        };
        self.current_step = step;
        for (x, r) in xs {
            send(x, r);
        }
    }
}
