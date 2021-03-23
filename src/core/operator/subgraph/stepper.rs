use crate::core::is_map::IsAddMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::node::{Node, NodeInfo};
use crate::core::operator::Op;
use crate::core::Step;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

pub(super) trait IsStepper<S> {
    fn flow(&mut self, step: &Step);
    fn min_key(&self) -> Option<&S>;
    fn propagate(&mut self, key: &S);
}

pub(super) struct Stepper<S, D, R, C: Op<D = (S, D), R = R>> {
    pending: BTreeMap<S, HashMap<D, R>>,
    input: Rc<RefCell<HashMap<(S, D), R>>>,
    output: Node<C>,
}

impl<S, D, R, C: Op<D = (S, D), R = R>> Stepper<S, D, R, C> {
    pub(super) fn new(
        pending: BTreeMap<S, HashMap<D, R>>,
        input: Rc<RefCell<HashMap<(S, D), R>>>,
        output: Node<C>,
    ) -> Self {
        Stepper {
            pending,
            input,
            output,
        }
    }
    pub(super) fn node_ref(&self) -> &Rc<RefCell<NodeInfo>> {
        &self.output.info
    }
}

impl<S: Key + Ord, D: Key, R: Monoid, C: Op<D = (S, D), R = R>> IsStepper<S>
    for Stepper<S, D, R, C>
{
    fn flow(&mut self, step: &Step) {
        let Stepper {
            pending,
            input: _,
            output,
        } = self;
        output.flow(step, |x, r| pending.add(x, r))
    }
    fn min_key(&self) -> Option<&S> {
        self.pending.first_key_value().map(|(k, _)| k)
    }
    fn propagate(&mut self, key: &S) {
        if let Some(popped) = self.pending.remove(key) {
            let mut input = self.input.borrow_mut();
            for (x, r) in popped {
                input.add((key.clone(), x), r)
            }
        }
    }
}
