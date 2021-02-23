pub fn fst<A, B>((a, _): (A, B)) -> A {
    a
}
pub fn snd<A, B>((_, b): (A, B)) -> B {
    b
}
pub fn swap<A, B>((a, b): (A, B)) -> (B, A) {
    (b, a)
}
