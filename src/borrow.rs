use std::ops::Deref;

pub enum MBorrowed<'a, T> {
    Borrowed(&'a T),
    Owned(T),
}

impl<'a, T> Deref for MBorrowed<'a, T> {
    type Target = T;

    fn deref<'b>(&'b self) -> &'b T {
        match self {
            Self::Borrowed(r) => r,
            Self::Owned(ref r) => r,
        }
    }
}

pub trait BorrowOrDefault<'a, T> {
    fn borrow_or_default(self) -> MBorrowed<'a, T>;
}

impl<'a, T: Default> BorrowOrDefault<'a, T> for Option<&'a T> {
    fn borrow_or_default(self) -> MBorrowed<'a, T> {
        self.map(MBorrowed::Borrowed)
            .unwrap_or(MBorrowed::Owned(Default::default()))
    }
}
