use crate::{
    map::{IsAddMap, IsMap},
    ArrangementG, CreationContext, ExecutionContext, Input, Op,
};
use std::collections::HashMap;

#[must_use = "This connection will be ignored unless it is handed off to a begin_feedback call"]
pub struct LeafConnection<C: Op, M: IsAddMap<C::D, C::R>> {
    from: ArrangementG<C, M>,
    to: Input<C::D, C::R>,
}

trait IsLeafConnection {
    fn feed(&self, context: &ExecutionContext) -> bool;
}

impl<C: Op, M: IsMap<C::D, C::R> + IsAddMap<C::D, C::R>> IsLeafConnection for LeafConnection<C, M> {
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

#[must_use = "This connection will be ignored unless it is handed off to a begin_feedback call"]
pub struct SimulConnection(Vec<Box<dyn IsLeafConnection>>);

impl SimulConnection {
    fn together_with(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        self
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

pub enum InterConnection {
    Simul(SimulConnection),
    Interrupt(Box<dyn IsArrangement>),
}

#[must_use = "This connection will be ignored unless it is handed off to a begin_feedback call"]
pub struct Connection(Vec<InterConnection>);

impl Connection {
    fn and_then(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        self
    }
}

pub trait IsSimulConnection: IsInterConnection {
    fn to_simul_connection(self) -> SimulConnection;
}

pub fn together_with<Lhs: IsSimulConnection, Rhs: IsSimulConnection>(
    lhs: Lhs,
    rhs: Rhs,
) -> SimulConnection {
    lhs.to_simul_connection()
        .together_with(rhs.to_simul_connection())
}

impl<C: Op, M: IsMap<C::D, C::R> + IsAddMap<C::D, C::R> + 'static> IsSimulConnection
    for LeafConnection<C, M>
{
    fn to_simul_connection(self) -> SimulConnection {
        SimulConnection(vec![Box::new(self)])
    }
}

impl IsSimulConnection for SimulConnection {
    fn to_simul_connection(self) -> SimulConnection {
        self
    }
}

pub trait IsInterConnection: IsConnection {
    fn to_inter_connection(self) -> InterConnection;
}

impl<C: Op, M: IsMap<C::D, C::R> + IsAddMap<C::D, C::R> + 'static> IsInterConnection
    for LeafConnection<C, M>
{
    fn to_inter_connection(self) -> InterConnection {
        self.to_simul_connection().to_inter_connection()
    }
}

impl IsInterConnection for SimulConnection {
    fn to_inter_connection(self) -> InterConnection {
        InterConnection::Simul(self)
    }
}

impl IsInterConnection for InterConnection {
    fn to_inter_connection(self) -> InterConnection {
        self
    }
}

pub trait IsConnection {
    fn to_connection(self) -> Connection;
}

pub fn and_then<Lhs: IsConnection, Rhs: IsConnection>(lhs: Lhs, rhs: Rhs) -> Connection {
    lhs.to_connection().and_then(rhs.to_connection())
}

impl<C: Op, M: IsMap<C::D, C::R> + IsAddMap<C::D, C::R> + 'static> IsConnection
    for LeafConnection<C, M>
{
    fn to_connection(self) -> Connection {
        self.to_simul_connection().to_connection()
    }
}

impl IsConnection for SimulConnection {
    fn to_connection(self) -> Connection {
        self.to_inter_connection().to_connection()
    }
}

impl IsConnection for InterConnection {
    fn to_connection(self) -> Connection {
        Connection(vec![self])
    }
}

impl IsConnection for Connection {
    fn to_connection(self) -> Connection {
        self
    }
}

impl<C: Op, M: IsAddMap<C::D, C::R>> ArrangementG<C, M> {
    pub fn feedback_gen(self, inp: Input<C::D, C::R>) -> LeafConnection<C, M> {
        LeafConnection {
            from: self,
            to: inp,
        }
    }
}

impl<C: Op> ArrangementG<C, HashMap<C::D, C::R>> {
    pub fn feedback(self, inp: Input<C::D, C::R>) -> LeafConnection<C, HashMap<C::D, C::R>> {
        self.feedback_gen(inp)
    }
}

pub struct FeedbackContext {
    context: ExecutionContext,
    connection: Connection,
}

impl CreationContext {
    pub fn begin_feedback<C: IsConnection>(self, connection: C) -> FeedbackContext {
        let context = self.begin();
        FeedbackContext {
            context,
            connection: connection.to_connection(),
        }
    }
}

impl FeedbackContext {
    pub fn get(&self) -> &ExecutionContext {
        &self.context
    }
    pub fn commit(&mut self) {
        self.context.commit();
        'outer: loop {
            for inter in self.connection.0.iter() {
                match inter {
                    InterConnection::Interrupt(arrangement) => {
                        if !arrangement.is_empty(&self.context) {
                            return;
                        }
                    }
                    InterConnection::Simul(simul) => {
                        let mut changed = false;
                        for x in simul.0.iter() {
                            changed |= x.feed(&self.context);
                        }
                        if changed {
                            self.context.commit();
                            continue 'outer;
                        }
                    }
                }
            }
            break;
        }
    }
}

impl<M: IsAddMap<C::D, C::R> + 'static, C: Op> ArrangementG<C, M> {
    pub fn interrupt(&self) -> InterConnection {
        InterConnection::Interrupt(Box::new(self.clone()))
    }
}
