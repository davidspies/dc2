use super::{
    node::{NodeInfo, NodeMaker},
    operator::InputRef,
    Step, TrackedId,
};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    io::{self, Write},
    mem,
    rc::Rc,
    sync::atomic,
    sync::atomic::AtomicU64,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> ContextId {
    NEXT_ID.fetch_add(1, atomic::Ordering::SeqCst)
}

pub(super) type ContextId = u64;

pub struct CreationContext {
    pub(super) context_id: ContextId,
    pub(super) node_maker: NodeMaker,
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
    pub(super) step: Step,
    pub(super) context_id: ContextId,
    infos: Vec<Rc<RefCell<NodeInfo>>>,
    pub(super) tracking_id: Option<TrackedId>,
    pub(super) tracked: RefCell<HashMap<TrackedId, HashSet<InputRef>>>,
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
            let name = match info.name.as_ref() {
                Some(name) => {
                    format!("{} <br/>", name)
                }
                None => "".to_string(),
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
