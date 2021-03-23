mod contextual;
mod leave;
mod registrar;
mod simple_input;
mod stepper;
mod variable;

use self::contextual::IsContext;
use self::registrar::Registrar;
pub use self::variable::Variable;
use crate::core::node::{NodeInfo, NodeMaker};
use crate::core::{key::Key, Dep};
use crate::core::{ContextId, CreationContext};
use std::cell::{Ref, RefCell};
use std::rc::Rc;

impl<'a, Ctx: IsContext, S: Key + Ord> IsContext for SubContext<'a, Ctx, S> {
    fn get_context_id(&self) -> ContextId {
        self.context_id
    }
    fn get_depth() -> usize {
        Ctx::get_depth() + 1
    }
    fn get_node_maker(&self) -> &NodeMaker {
        &self.node_maker
    }
}
pub struct SubContext<'a, Ctx, S: Key + Ord> {
    parent: &'a Ctx,
    registrar: Registrar<S>,
    context_id: ContextId,
    node_maker: NodeMaker,
}
pub struct Finalizer<'a, Ctx, S: Key + Ord> {
    parent: &'a Ctx,
    registrar: Registrar<S>,
    node_maker: NodeMaker,
}

impl<Ctx: IsContext, S: Key + Ord> Finalizer<'_, Ctx, S> {
    fn node_ref(&self) -> Ref<Rc<RefCell<NodeInfo>>> {
        Ref::map(self.registrar.get_inner(), |x| &x.info)
    }
    fn dep(&self) -> Dep {
        let node_info = self.node_ref();
        node_info.borrow_mut().depth = Ctx::get_depth();
        Dep {
            context_id: self.parent.get_context_id(),
            node_info: Rc::downgrade(&node_info),
            node_maker: self.node_maker.clone(),
        }
    }
}

impl CreationContext {
    /// Creates a recursive subgraph. The template parameter is the "step" type. Dependencies must
    /// tiered by the step type's `Ord` implementation or else weird things can happen such as
    /// "phantom" records hanging around after they've been deleted or calls to `read`
    /// not halting.
    ///
    /// Example:
    ///
    /// Here we set the `step` parameter to () and so the subgraph isn't properly tiered:
    /// ```
    /// use dc2::{CreationContext, Op, Relation};
    /// use std::collections::HashMap;
    ///
    /// let mut creation = CreationContext::new();
    /// let (verts_inp, verts) = creation.create_input::<char, _>();
    /// let (edges_inp, edges) = creation.create_input::<(char, char), _>();
    /// let mut subcontext = creation.subgraph::<()>();
    /// let (trans_var, trans_sub) = subcontext.variable::<(char, char), _>();
    /// let next = verts
    ///     .map(|v| (v, v))
    ///         .concat(
    ///             trans_sub
    ///             .map(|((), (x, y))| (y, x))
    ///                 .join(edges)
    ///                 .map(|(_, (x, y))| (x, y)),
    ///         )
    ///         .distinct()
    ///         .split();
    /// trans_var.set(next.clone().map(|e| ((), e)));
    /// let trans = next
    ///     .leave(&subcontext.finish())
    ///     .get_arrangement::<HashMap<(char, char), _>>(&creation);
    ///
    /// let mut context = creation.begin();
    ///
    /// verts_inp.insert(&context, 'a');
    /// verts_inp.insert(&context, 'b');
    /// verts_inp.insert(&context, 'c');
    /// edges_inp.insert(&context, ('a', 'b'));
    /// edges_inp.insert(&context, ('b', 'c'));
    /// edges_inp.insert(&context, ('c', 'b'));
    /// context.commit();
    ///
    /// assert!(trans.read(&context).contains_key(&(('a', 'c'))));
    ///
    /// edges_inp.delete(&context, ('a', 'b'));
    /// context.commit();
    ///
    /// // Even though we deleted the ('a', 'b') edge, the transitive closure still claims to have
    /// // a path from 'a' to 'c'.
    /// assert!(trans.read(&context).contains_key(&(('a', 'c'))));
    /// ```
    ///
    /// To do this correctly, we will tier the graph by the minimum distance between vertices:
    /// ```
    /// use dc2::{CreationContext, Op, Relation};
    /// use std::collections::HashMap;
    ///
    /// let mut creation = CreationContext::new();
    /// let (verts_inp, verts) = creation.create_input::<char, _>();
    /// let (edges_inp, edges) = creation.create_input::<(char, char), _>();
    /// let mut subcontext = creation.subgraph::<usize>();
    /// let (trans_var, trans_sub) = subcontext.variable::<(char, char), _>();
    /// let next = verts
    ///     .map(|v| ((v, v), 0))
    ///     .concat(
    ///         trans_sub
    ///             .map(|(dist, (x, y))| (y, (x, dist)))
    ///             .join(edges)
    ///             .map(|(_, ((x, dist), y))| ((x, y), dist + 1)),
    ///     )
    ///     .group_min()
    ///     .split();
    /// trans_var.set(next.clone().map(|(e, dist)| (dist, e)));
    /// let trans = next
    ///     .leave(&subcontext.finish())
    ///     .get_arrangement::<HashMap<(char, char), HashMap<_, _>>>(&creation);
    ///
    /// let mut context = creation.begin();
    ///     
    /// verts_inp.insert(&context, 'a');
    /// verts_inp.insert(&context, 'b');
    /// verts_inp.insert(&context, 'c');
    /// edges_inp.insert(&context, ('a', 'b'));
    /// edges_inp.insert(&context, ('b', 'c'));
    /// edges_inp.insert(&context, ('c', 'b'));
    /// context.commit();
    ///
    /// assert!(trans.read(&context).contains_key(&(('a', 'c'))));
    ///
    /// edges_inp.delete(&context, ('a', 'b'));
    /// context.commit();
    ///
    /// // The ('a', 'c') edge has (correctly) been deleted once we removed the ('a', 'b') edge.
    /// assert!(!trans.read(&context).contains_key(&(('a', 'c'))));
    /// ```
    ///
    pub fn subgraph<'a, S: Key + Ord>(&'a mut self) -> SubContext<'a, Self, S> {
        SubContext::from(self)
    }
}

impl<'a, Ctx: IsContext, S: Key + Ord> SubContext<'a, Ctx, S> {
    fn from(parent: &'a Ctx) -> Self {
        SubContext {
            parent,
            registrar: Registrar::new_registrar(Ctx::get_depth(), parent.get_node_maker()),
            context_id: parent.get_context_id(),
            node_maker: parent.get_node_maker().clone(),
        }
    }
    pub fn subgraph<'b, T: Key + Ord>(&'b mut self) -> SubContext<'b, Self, T> {
        SubContext::from(self)
    }
    pub fn finish(self) -> Finalizer<'a, Ctx, S> {
        Finalizer {
            parent: self.parent,
            registrar: self.registrar,
            node_maker: self.node_maker,
        }
    }
}
