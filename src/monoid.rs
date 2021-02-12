use crate::emptyable::Emptyable;
use std::ops::{AddAssign, Neg, Sub};

pub trait Monoid:
    Clone + Emptyable + AddAssign<Self> + Sub<Self, Output = Self> + Neg<Output = Self> + 'static
{
    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}

impl<
        R: Clone
            + Emptyable
            + AddAssign<Self>
            + Sub<Self, Output = Self>
            + Neg<Output = Self>
            + 'static,
    > Monoid for R
{
}
