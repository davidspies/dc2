mod arrangement;
pub mod borrow;
pub mod emptyable;
pub mod is_map;
pub mod iter;
pub mod key;
pub mod monoid;
mod node;
mod operator;

pub use self::arrangement::{Arrangement, ArrangementG};
use self::node::{Node, NodeInfo, NodeMaker};
use self::operator::InputRef;
pub use self::operator::{DynOp, Input, IsReduce, Op, Receiver, ReduceOutput};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    io::{self, Write},
    mem,
    rc::Rc,
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

type TrackedId = usize;

pub struct ExecutionContext {
    step: usize,
    context_id: ContextId,
    infos: Vec<Rc<RefCell<NodeInfo>>>,
    tracking_id: Option<TrackedId>,
    tracked: RefCell<HashMap<TrackedId, HashSet<InputRef>>>,
}

impl CreationContext {
    pub fn begin(self) -> ExecutionContext {
        let infos: Vec<Rc<RefCell<NodeInfo>>> = mem::take(&mut self.node_maker.infos.borrow_mut());
        ExecutionContext {
            step: 0,
            context_id: self.context_id,
            infos,
            tracking_id: None,
            tracked: RefCell::new(HashMap::new()),
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
                "  node{} [label=< {} {} <br/> {} >];",
                info.relation_id, name, info.operator_name, info.message_count
            )?;
        }
        for info_ref in self.infos.iter() {
            let info = info_ref.borrow();
            if !info.shown {
                continue;
            }
            for dep in info.deps.iter() {
                let dep_info = dep.borrow();
                writeln!(
                    file,
                    "  node{} -> node{};",
                    dep_info.shown_relation_id(),
                    info.relation_id,
                )?;
            }
        }
        writeln!(file, "}}")
    }
    pub fn with_temp_changes<Changes: FnOnce(&mut Self), Cont: FnOnce(&mut Self)>(
        &mut self,
        changes: Changes,
        cont: Cont,
    ) {
        self.commit();
        let tracking_id = self.step;
        let prev_tracking_id = self.tracking_id;
        self.tracking_id = Some(tracking_id);
        changes(self);
        self.tracking_id = prev_tracking_id;
        self.commit();
        cont(self);
        for inp in self
            .tracked
            .borrow_mut()
            .remove(&tracking_id)
            .unwrap_or_default()
        {
            inp.undo_changes(self.step, tracking_id);
        }
        self.commit();
    }
}

type Step = usize;

#[derive(Clone)]
pub struct Relation<C: ?Sized> {
    context_id: ContextId,
    node_maker: NodeMaker,
    inner: Node<C>,
}

struct Dep {
    context_id: usize,
    node_info: Rc<RefCell<NodeInfo>>,
    node_maker: NodeMaker,
}

impl<C: Op> Relation<C> {
    fn new(deps: Vec<Dep>, inner: C) -> Self {
        let context_id = deps[0].context_id;
        for dep in &deps[1..] {
            assert_eq!(dep.context_id, context_id, "Context mismatch")
        }
        let node_maker = deps[0].node_maker.clone();
        Relation {
            inner: node_maker.make_node(deps.into_iter().map(|x| x.node_info).collect(), inner),
            context_id,
            node_maker,
        }
    }
    fn dep(&self) -> Dep {
        Dep {
            context_id: self.context_id,
            node_info: Rc::clone(self.node_ref()),
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
    fn with_input(self, input: InputRef) -> Self {
        self.inner.info.borrow_mut().inputs.insert(input);
        self
    }
}
