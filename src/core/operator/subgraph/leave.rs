use super::contextual::IsContext;
use super::registrar::Registrar;
use super::Finalizer;
use crate::core::key::Key;
use crate::core::node::Node;
use crate::core::operator::Op;
use crate::core::{Relation, Step};

struct Leave<S: Key + Ord, C> {
    inner: Node<C>,
    registrar: Registrar<S>,
}

impl<S: Key + Ord, C: Op> Op for Leave<S, C> {
    type D = C::D;
    type R = C::R;
    fn default_op_name() -> &'static str {
        "leave"
    }
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: &Step, send: F) {
        self.registrar.flow(step, |_, _| ());
        self.inner
            .flow(&step.append(self.registrar.get_inner_step()), send)
    }
}

impl<'b, C: Op> Relation<'b, C> {
    pub fn leave<'a: 'b, Ctx: IsContext + 'a, S: Key + Ord>(
        self,
        finalizer: &Finalizer<'b, Ctx, S>,
    ) -> Relation<'a, impl Op<D = C::D, R = C::R>> {
        assert_eq!(
            self.context_id,
            finalizer.parent.get_context_id(),
            "Context mismatch"
        );
        Relation::new(
            vec![finalizer.dep(), self.dep()],
            Leave {
                inner: self.inner,
                registrar: finalizer.registrar.clone(),
            },
        )
        .with_depth(Ctx::get_depth())
    }
}
