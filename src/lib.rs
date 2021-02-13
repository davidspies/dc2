#![feature(map_first_last)]

mod convenience_operators;
mod core;

use self::core::Op;
pub use self::core::Relation;

#[cfg(test)]
mod tests;
