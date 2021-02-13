mod join;
mod map;

use std::ops::AddAssign;

pub trait Op {
    type D: 'static;
    type R: AddAssign<Self::R>;
}
