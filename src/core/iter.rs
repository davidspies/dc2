pub use std::iter::*;

pub struct TupleWith<I: Iterator, T: Clone> {
    iter: Peekable<I>,
    snd: Option<T>,
}

impl<I: Iterator, T: Clone> Iterator for TupleWith<I, T> {
    type Item = (I::Item, T);

    fn next(&mut self) -> Option<Self::Item> {
        let ox = self.iter.next();
        ox.map(|x| {
            let snd = if self.iter.peek().is_some() {
                self.snd.as_ref().unwrap().clone()
            } else {
                self.snd.take().unwrap()
            };
            (x, snd)
        })
    }
}

pub trait TupleableWith: Iterator + Sized {
    /// Equivalent to .map(|x| (x, snd.clone())) but moves the last element rather than cloning it.
    fn tuple_with<T: Clone>(self, snd: T) -> TupleWith<Self, T>;
}

impl<I: Iterator> TupleableWith for I {
    fn tuple_with<T: Clone>(self, snd: T) -> TupleWith<Self, T> {
        TupleWith {
            iter: self.peekable(),
            snd: Some(snd),
        }
    }
}
