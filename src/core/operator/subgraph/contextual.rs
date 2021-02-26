use crate::core::node::NodeMaker;
use crate::core::{ContextId, CreationContext};

pub trait IsContext {
    fn get_context_id(&self) -> ContextId;
    fn get_depth() -> usize;
    fn get_node_maker(&self) -> &NodeMaker;
}
impl IsContext for CreationContext {
    fn get_context_id(&self) -> ContextId {
        self.context_id
    }
    fn get_depth() -> usize {
        0
    }
    fn get_node_maker(&self) -> &NodeMaker {
        &self.node_maker
    }
}
