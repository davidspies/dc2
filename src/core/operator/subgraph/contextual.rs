use crate::core::{ContextId, CreationContext};

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
