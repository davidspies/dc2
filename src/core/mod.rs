mod arrangement;
pub mod borrow;
pub mod emptyable;
pub mod is_map;
pub mod iter;
pub mod key;
pub mod monoid;
mod node;
mod operator;

pub use self::arrangement::Arrangement;
use self::node::{Node, NodeInfo, NodeMaker};
pub use self::operator::{subgraph, DynOp, Input, IsReduce, Op, Receiver, ReduceOutput};
use std::{
    cell::RefCell,
    io::{self, Write},
    marker::PhantomData,
    mem,
    rc::{Rc, Weak},
    sync::atomic,
    sync::atomic::AtomicUsize,
};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn next_id() -> ContextId {
    NEXT_ID.fetch_add(1, atomic::Ordering::SeqCst)
}

type ContextId = usize;

pub struct CreationContext {
    context_id: ContextId,
    node_maker: NodeMaker,
}

impl CreationContext {
    pub fn new() -> Self {
        CreationContext {
            context_id: next_id(),
            node_maker: NodeMaker::new(),
        }
    }
}

pub struct ExecutionContext {
    step: usize,
    context_id: ContextId,
    infos: Vec<Rc<RefCell<NodeInfo>>>,
}

impl CreationContext {
    pub fn begin(self) -> ExecutionContext {
        let infos: Vec<Rc<RefCell<NodeInfo>>> = mem::take(&mut self.node_maker.infos.borrow_mut());
        ExecutionContext {
            step: 0,
            context_id: self.context_id,
            infos,
        }
    }
}

impl ExecutionContext {
    pub fn commit(&mut self) {
        self.step += 1;
    }
    pub fn dump_dot<W: Write>(&self, file: &mut W) -> Result<(), io::Error> {
        writeln!(file, "digraph flow {{")?;
        for info_ref in self.infos.iter() {
            let info = info_ref.borrow();
            let subgraph = if info.depth > 0 {
                ",style=filled,fillcolor=lightblue"
            } else {
                ""
            };
            if !info.shown {
                continue;
            }
            let name = if let Some(name) = info.name.as_ref() {
                format!("{} <br/>", name)
            } else {
                "".to_string()
            };
            writeln!(
                file,
                "  node{} [label=< {} {} <br/> {} >{}];",
                info.relation_id, name, info.operator_name, info.message_count, subgraph
            )?;
        }
        for info_ref in self.infos.iter() {
            let info = info_ref.borrow();
            if !info.shown {
                continue;
            }
            for dep in info.deps.iter() {
                let dep_ptr = dep.upgrade().unwrap();
                let dep_info = dep_ptr.borrow();
                let label = if dep_info.is_registrar {
                    " [style=dotted]"
                } else {
                    ""
                };
                writeln!(
                    file,
                    "  node{} -> node{}{};",
                    dep_info.shown_relation_id(),
                    info.relation_id,
                    label
                )?;
            }
        }
        writeln!(file, "}}")
    }
}

pub struct Sub<'a> {
    pub depth: usize,
    pub step: usize,
    pub parent: &'a Step<'a>,
}

pub enum Step<'a> {
    Root(usize),
    Sub(Sub<'a>),
}

impl<'a> Step<'a> {
    fn get_last(&self) -> usize {
        match self {
            &Step::Root(s) => s,
            &Step::Sub(Sub { step, .. }) => step,
        }
    }
    fn step_for(&self, depth: usize) -> &Step {
        match self {
            &Self::Root(_) => {
                assert_eq!(depth, 0);
                self
            }
            &Self::Sub(Sub {
                depth: my_depth,
                step: _,
                ref parent,
            }) => {
                if my_depth == depth {
                    self
                } else {
                    assert!(depth < my_depth);
                    parent.step_for(depth)
                }
            }
        }
    }
    fn get_depth(&self) -> usize {
        match self {
            &Self::Root(_) => 0,
            &Self::Sub(Sub { depth, .. }) => depth,
        }
    }
    fn append(&'a self, step: usize) -> Step<'a> {
        Self::Sub(Sub {
            depth: self.get_depth() + 1,
            step,
            parent: self,
        })
    }
}

#[derive(Clone)]
pub struct Relation<'a, C: ?Sized> {
    context_id: ContextId,
    phantom: PhantomData<&'a ()>,
    node_maker: NodeMaker,
    inner: Node<C>,
}

struct Dep {
    context_id: usize,
    node_info: Weak<RefCell<NodeInfo>>,
    node_maker: NodeMaker,
}

impl<'a, C: Op> Relation<'a, C> {
    fn new(deps: Vec<Dep>, inner: C) -> Self {
        let context_id = deps[0].context_id;
        for dep in &deps[1..] {
            assert_eq!(dep.context_id, context_id, "Context mismatch")
        }
        let node_maker = deps[0].node_maker.clone();
        Relation {
            inner: node_maker.make_node(deps.into_iter().map(|x| x.node_info).collect(), inner),
            context_id,
            phantom: PhantomData,
            node_maker,
        }
    }
    fn dep(&self) -> Dep {
        Dep {
            context_id: self.context_id,
            node_info: self.node_ref(),
            node_maker: self.node_maker.clone(),
        }
    }
    pub fn named(mut self, name: &str) -> Self {
        self.inner.set_name(name.to_string());
        self
    }
    pub fn op_named(mut self, name: &str) -> Self {
        self.inner.set_op_name(name.to_string());
        self
    }
    pub fn hidden(self) -> Self {
        assert!(
            self.inner.info.borrow().hideable,
            "Unhideable relation type"
        );
        assert_eq!(
            self.inner.info.borrow().deps.len(),
            1,
            "Too many inputs to hide"
        );
        self.set_shown(false)
    }
    pub fn shown(self) -> Self {
        self.set_shown(true)
    }
    fn set_shown(self, shown: bool) -> Self {
        self.inner.info.borrow_mut().shown = shown;
        self
    }
    fn with_depth(self, depth: usize) -> Self {
        self.inner.info.borrow_mut().depth = depth;
        self
    }
    fn depth(&self) -> usize {
        self.inner.info.borrow().depth
    }
}
