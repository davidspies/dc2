mod arrangement;
pub mod borrow;
pub(super) mod context;
pub mod emptyable;
pub mod is_map;
pub mod iter;
pub mod key;
pub mod monoid;
mod node;
mod operator;
mod relation;

pub use self::arrangement::{Arrangement, ArrangementG};
pub use self::operator::{DynOp, Input, IsReduce, Op, Receiver, ReduceOutput};
pub use self::relation::Relation;

type TrackedId = Step;
type Step = u64;
