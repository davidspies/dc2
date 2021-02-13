mod operator;

pub use self::operator::Op;

#[derive(Clone)]
pub struct Relation<C> {
    inner: C,
}
