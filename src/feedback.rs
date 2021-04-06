use crate::{
    key::Key,
    map::{IsAddMap, IsMap},
    ArrangementG, CreationContext, ExecutionContext, Input, Op,
};
use std::collections::{BTreeMap, HashMap};

pub struct FeedbackCreationContext {
    context: CreationContext,
    connections: Vec<Connection>,
}

impl FeedbackCreationContext {
    pub fn get(&self) -> &CreationContext {
        &self.context
    }
}

struct LeafConnection<C: Op, M: IsAddMap<C::D, C::R>> {
    from: ArrangementG<C, M>,
    to: Input<C::D, C::R>,
}

struct OrderedLeafConnection<K: Ord, V, C: Op<D = (K, V)>, M: IsAddMap<V, C::R>> {
    from: ArrangementG<C, BTreeMap<K, M>>,
    to: Input<C::D, C::R>,
}

trait IsConnection {
    fn feed(&self, context: &ExecutionContext) -> bool;
}

impl<C: Op, M: IsMap<C::D, C::R> + IsAddMap<C::D, C::R>> IsConnection for LeafConnection<C, M> {
    fn feed(&self, context: &ExecutionContext) -> bool {
        let out_map = self.from.read(context);
        if out_map.is_empty() {
            false
        } else {
            out_map.foreach(|x, r| {
                self.to.update(context, x.clone(), r.clone());
            });
            true
        }
    }
}

impl<K: Key + Ord, V: Key, C: Op<D = (K, V)>, M: IsMap<V, C::R> + IsAddMap<V, C::R>> IsConnection
    for OrderedLeafConnection<K, V, C, M>
{
    fn feed(&self, context: &ExecutionContext) -> bool {
        let out_map = self.from.read(context);
        match out_map.first_key_value() {
            Some((k, m)) => {
                m.foreach(|x, r| self.to.update(context, (k.clone(), x.clone()), r.clone()));
                true
            }
            None => false,
        }
    }
}

pub trait IsArrangement {
    fn is_empty(&self, context: &ExecutionContext) -> bool;
}

impl<M: IsAddMap<C::D, C::R>, C: Op> IsArrangement for ArrangementG<C, M> {
    fn is_empty(&self, context: &ExecutionContext) -> bool {
        self.read(context).is_empty()
    }
}

enum Connection {
    Conn(Box<dyn IsConnection>),
    Interrupt(Box<dyn IsArrangement>),
}

impl<C: Op, M: 'static + IsMap<C::D, C::R> + IsAddMap<C::D, C::R>> ArrangementG<C, M> {
    pub fn feedback_gen(self, inp: Input<C::D, C::R>, context: &mut FeedbackCreationContext) {
        context
            .connections
            .push(Connection::Conn(Box::new(LeafConnection {
                from: self,
                to: inp,
            })))
    }
}
impl<K: Ord + Key, V: Key, C: Op<D = (K, V)>, M: 'static + IsMap<V, C::R> + IsAddMap<V, C::R>>
    ArrangementG<C, BTreeMap<K, M>>
{
    pub fn step_feedback_gen(
        self,
        inp: Input<(K, V), C::R>,
        context: &mut FeedbackCreationContext,
    ) {
        context
            .connections
            .push(Connection::Conn(Box::new(OrderedLeafConnection {
                from: self,
                to: inp,
            })))
    }
}

impl<C: Op> ArrangementG<C, HashMap<C::D, C::R>> {
    pub fn feedback(self, inp: Input<C::D, C::R>, context: &mut FeedbackCreationContext) {
        self.feedback_gen(inp, context)
    }
}
impl<K: Ord + Key, V: Key, C: Op<D = (K, V)>> ArrangementG<C, BTreeMap<K, HashMap<V, C::R>>> {
    pub fn step_feedback(self, inp: Input<(K, V), C::R>, context: &mut FeedbackCreationContext) {
        self.step_feedback_gen(inp, context)
    }
}

pub struct FeedbackContext {
    context: ExecutionContext,
    connections: Vec<Connection>,
}

pub struct FeedbackContextRef<'a> {
    context: &'a mut ExecutionContext,
    connections: &'a Vec<Connection>,
}

pub trait IsFeedbackContext {
    fn get(&self) -> &ExecutionContext;
    fn commit(&mut self);
    fn with_temp_changes<
        Changes: for<'a> FnOnce(FeedbackContextRef<'a>),
        Cont: for<'a> FnOnce(FeedbackContextRef<'a>),
    >(
        &mut self,
        changes: Changes,
        cont: Cont,
    );
}

impl FeedbackCreationContext {
    pub fn begin_feedback(self) -> FeedbackContext {
        let context = self.context.begin();
        FeedbackContext {
            context,
            connections: self.connections,
        }
    }
}

impl FeedbackContext {
    fn as_ref(&mut self) -> FeedbackContextRef {
        FeedbackContextRef {
            context: &mut self.context,
            connections: &self.connections,
        }
    }
}

impl IsFeedbackContext for FeedbackContext {
    fn get(&self) -> &ExecutionContext {
        &self.context
    }
    fn commit(&mut self) {
        self.as_ref().commit()
    }
    fn with_temp_changes<
        Changes: for<'a> FnOnce(FeedbackContextRef<'a>),
        Cont: for<'a> FnOnce(FeedbackContextRef<'a>),
    >(
        &mut self,
        changes: Changes,
        cont: Cont,
    ) {
        self.as_ref().with_temp_changes(changes, cont)
    }
}

impl IsFeedbackContext for FeedbackContextRef<'_> {
    fn get(&self) -> &ExecutionContext {
        self.context
    }
    fn commit(&mut self) {
        self.context.commit();
        'outer: loop {
            for inter in self.connections.iter() {
                match inter {
                    Connection::Interrupt(arrangement) => {
                        if !arrangement.is_empty(&self.context) {
                            return;
                        }
                    }
                    Connection::Conn(conn) => {
                        if conn.feed(self.context) {
                            self.context.commit();
                            continue 'outer;
                        }
                    }
                }
            }
            break;
        }
    }
    fn with_temp_changes<
        Changes: for<'a> FnOnce(FeedbackContextRef<'a>),
        Cont: for<'a> FnOnce(FeedbackContextRef<'a>),
    >(
        &mut self,
        changes: Changes,
        cont: Cont,
    ) {
        self.commit();
        let FeedbackContextRef {
            context,
            connections,
        } = self;
        context.with_temp_changes(
            |context| {
                changes(FeedbackContextRef {
                    context,
                    connections,
                });
                FeedbackContextRef {
                    context,
                    connections,
                }
                .commit();
            },
            |context| {
                cont(FeedbackContextRef {
                    context,
                    connections,
                });
                FeedbackContextRef {
                    context,
                    connections,
                }
                .commit();
            },
        );
    }
}

impl<M: IsAddMap<C::D, C::R> + 'static, C: Op> ArrangementG<C, M> {
    pub fn interrupt(self, context: &mut FeedbackCreationContext) {
        context
            .connections
            .push(Connection::Interrupt(Box::new(self)));
    }
}
