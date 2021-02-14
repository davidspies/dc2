use super::stepper::{IsStepper, Stepper};
use crate::core::key::Key;
use crate::core::operator::{Op, Receiver};
use crate::core::Step;

pub(super) struct RegistrarInner<S> {
    steppers: Vec<Box<dyn IsStepper<S>>>,
    inner_step: usize,
}

pub(super) type Registrar<S> = Receiver<RegistrarInner<S>>;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) enum Void {}

impl<S: Key + Ord> Op for RegistrarInner<S> {
    type D = Void;
    type R = isize;

    fn flow<Send>(&mut self, step: &Step, _send: Send) {
        loop {
            self.inner_step += 1;
            let next_step = step.append(self.inner_step);
            let mut min_key: Option<&S> = None;
            for stepper in self.steppers.iter_mut() {
                stepper.flow(&next_step);
                min_key = match (min_key, stepper.min_key()) {
                    (Some(l), Some(r)) => Some(l.min(r)),
                    (l, r) => l.or(r),
                };
            }
            if let Some(min_key) = min_key.map(Clone::clone) {
                for stepper in self.steppers.iter_mut() {
                    stepper.propagate(&min_key)
                }
            } else {
                break;
            }
        }
    }
}

impl<S: Key + Ord> Registrar<S> {
    pub(super) fn new_registrar(depth: usize) -> Self {
        Receiver::new(
            RegistrarInner {
                steppers: Vec::new(),
                inner_step: 0,
            },
            depth,
        )
    }
    pub(super) fn add_stepper<D: Key, C: Op<D = (S, D)>>(
        &mut self,
        stepper: Stepper<S, D, C::R, C>,
    ) {
        self.get_inner_mut().steppers.push(Box::new(stepper))
    }
    pub(super) fn get_inner_step(&self) -> usize {
        self.get_inner().inner_step
    }
}
