use super::split::Receiver;
use super::Op;
use crate::core::is_map::IsAddMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::{ContextId, CreationContext, Relation, Step};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

pub trait IsContext {
    fn get_context_id(&self) -> ContextId;
    fn get_depth() -> usize;
}

impl IsContext for CreationContext {
    fn get_context_id(&self) -> ContextId {
        self.0
    }
    fn get_depth() -> usize {
        0
    }
}
impl<'a, Ctx: IsContext, S: Clone + Ord> IsContext for SubContext<'a, Ctx, S> {
    fn get_context_id(&self) -> ContextId {
        self.context_id
    }
    fn get_depth() -> usize {
        Ctx::get_depth() + 1
    }
}

pub struct SubContext<'a, Ctx, S: Clone + Ord> {
    parent: &'a Ctx,
    registrar: Registrar<S>,
    context_id: ContextId,
}
pub struct Finalizer<'a, Ctx, S: Clone + Ord> {
    parent: &'a Ctx,
    registrar: Registrar<S>,
}

trait IsStepper<S> {
    fn flow(&mut self, step: &Step);
    fn min_key(&self) -> Option<&S>;
    fn propagate(&mut self, key: &S);
}

struct SimpleInput<D, R>(Rc<RefCell<HashMap<D, R>>>);

impl<D: Key, R: Monoid> Op for SimpleInput<D, R> {
    type D = D;
    type R = R;
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, _step: &Step, mut send: F) {
        for (x, r) in mem::take(&mut *self.0.borrow_mut()) {
            send(x, r)
        }
    }
}

struct Stepper<S, D, R, C: Op<D = (S, D), R = R>> {
    pending: BTreeMap<S, HashMap<D, R>>,
    input: Rc<RefCell<HashMap<(S, D), R>>>,
    output: C,
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

struct RegistrarInner<S> {
    steppers: Vec<Box<dyn IsStepper<S>>>,
    inner_step: usize,
}

type Registrar<S> = Receiver<RegistrarInner<S>>;

#[derive(Clone, PartialEq, Eq, Hash)]
enum Void {}

impl<S: Clone + Ord> Op for RegistrarInner<S> {
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

impl CreationContext {
    pub fn subgraph<'a, S: Key + Ord>(&'a mut self) -> SubContext<'a, Self, S> {
        SubContext::from(self)
    }
}

pub struct Variable<'a, S: Clone + Ord, D, R> {
    inner: Rc<RefCell<HashMap<(S, D), R>>>,
    context_id: ContextId,
    registrar: Registrar<S>,
    phantom: PhantomData<&'a ()>,
}

impl<'a, Ctx: IsContext, S: Key + Ord> SubContext<'a, Ctx, S> {
    fn from(parent: &'a Ctx) -> Self {
        SubContext {
            parent,
            registrar: Receiver::new(
                RegistrarInner {
                    steppers: Vec::new(),
                    inner_step: 0,
                },
                Ctx::get_depth(),
            ),
            context_id: parent.get_context_id(),
        }
    }
    pub fn subgraph<'b, T: Key + Ord>(&'b mut self) -> SubContext<'b, Self, T> {
        SubContext::from(self)
    }
    pub fn variable<D: Key, R: Monoid>(
        &mut self,
    ) -> (Variable<'a, S, D, R>, Relation<impl Op<D = (S, D), R = R>>) {
        let rc = Rc::new(RefCell::new(HashMap::new()));
        (
            Variable {
                inner: rc.clone(),
                context_id: self.context_id,
                registrar: self.registrar.clone(),
                phantom: PhantomData,
            },
            Relation {
                inner: SimpleInput(rc),
                context_id: self.context_id,
                depth: Self::get_depth(),
                phantom: PhantomData,
            },
        )
    }
    pub fn finish(self) -> Finalizer<'a, Ctx, S> {
        Finalizer {
            parent: self.parent,
            registrar: self.registrar,
        }
    }
}

impl<'a, S: Key + Ord, D: Key, R: Monoid> Variable<'a, S, D, R> {
    pub fn set<C: Op<D = (S, D), R = R> + 'static>(self, rel: Relation<'a, C>) {
        assert_eq!(self.context_id, rel.context_id, "Context mismatch");
        self.registrar
            .get_inner_mut()
            .steppers
            .push(Box::new(Stepper {
                pending: BTreeMap::new(),
                input: self.inner,
                output: rel.inner,
            }));
    }
}

struct Leave<S: Clone + Ord, C> {
    inner: C,
    registrar: Registrar<S>,
}

impl<S: Clone + Ord, C: Op> Op for Leave<S, C> {
    type D = C::D;
    type R = C::R;
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: &Step, send: F) {
        self.registrar.flow(step, |_, _| ());
        self.inner
            .flow(&step.append(self.registrar.get_inner().inner_step), send)
    }
}

impl<'b, C: Op> Relation<'b, C> {
    pub fn leave<'a, Ctx: IsContext, S: 'static + Clone + Ord>(
        self,
        finalizer: &Finalizer<'b, Ctx, S>,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>>
    where
        'a: 'b,
        Ctx: 'a,
    {
        assert_eq!(
            self.context_id,
            finalizer.parent.get_context_id(),
            "Context mismatch"
        );
        Relation {
            inner: Leave {
                inner: self.inner,
                registrar: finalizer.registrar.clone(),
            },
            depth: Ctx::get_depth(),
            context_id: self.context_id,
            phantom: PhantomData,
        }
    }
}
