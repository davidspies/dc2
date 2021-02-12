mod borrow;
mod emptyable;
mod is_map;
mod iter;
mod key;
mod monoid;

use crate::borrow::BorrowOrDefault;
use crate::is_map::{IsAddMap, IsDiscardMap, IsMap, IsRemoveMap};
use crate::iter::TupleableWith;
use crate::key::Key;
use crate::monoid::Monoid;
use std::cell::RefCell;
use std::collections::{hash_map, HashMap, HashSet};
use std::marker::PhantomData;
use std::mem;
use std::ops::Mul;
use std::rc::{Rc, Weak};

pub struct Arrangement<D, R, C> {
    from: C,
    value: HashMap<D, R>,
}

pub trait DynOperator {
    type D: Key;
    type R: Monoid;
    fn flow_to(&mut self, step: usize) -> HashMap<Self::D, Self::R>;
}

fn default_flow_to<C: Operator>(this: &mut C, step: usize) -> HashMap<C::D, C::R> {
    let mut res = HashMap::new();
    this.flow(step, |x, r| res.add(x, r));
    res
}

pub trait Operator: DynOperator {
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: usize, send: F);
}

#[derive(Clone)]
pub struct CWrapper<C>(C);

pub type Collection<D, R> = CWrapper<WCollection<D, R>>;

impl<C: Operator> CWrapper<C> {
    pub fn flat_map_r<
        F: Fn(C::D, C::R) -> I,
        D2: Key,
        R2: Monoid,
        I: IntoIterator<Item = (D2, R2)>,
    >(
        self,
        f: F,
    ) -> CWrapper<impl Operator<D = D2, R = R2>> {
        CWrapper(FlatMap {
            inner: self.0,
            op: f,
        })
    }
    pub fn concat<C2: Operator<D = C::D, R = C::R>>(
        self,
        other: CWrapper<C2>,
    ) -> CWrapper<impl Operator<D = C::D, R = C::R>> {
        CWrapper(Concat {
            left: self.0,
            right: other.0,
        })
    }
    pub fn split(self) -> CWrapper<Receiver<C>> {
        let data = Rc::new(RefCell::new(HashMap::new()));
        let source = Rc::new(RefCell::new(Source {
            source: self.0,
            listeners: vec![Rc::downgrade(&data)],
            step: 0,
        }));
        CWrapper(Receiver { data, source })
    }
    pub fn collect(self) -> Collection<C::D, C::R>
    where
        C: 'static,
    {
        CWrapper(WCollection(Box::new(self.0)))
    }
}

impl<K: Key, D: Key, C: Operator<D = (K, D)>> CWrapper<C> {
    pub fn join<C2: Operator<D = (K, D2)>, D2: Key, OR: Monoid>(
        self,
        other: CWrapper<C2>,
    ) -> CWrapper<impl Operator<D = (K, D, D2), R = OR>>
    where
        C::R: Mul<C2::R, Output = OR>,
    {
        CWrapper(Join {
            left: self.0,
            right: other.0,
            left_map: HashMap::new(),
            right_map: HashMap::new(),
        })
    }
    pub fn reduce<
        D2: Key,
        R2: Monoid,
        F: Fn(K, &M1) -> M2,
        M1: IsAddMap<D, C::R>,
        M2: IsMap<D2, R2>,
    >(
        self,
        proc: F,
    ) -> CWrapper<impl Operator<D = (K, D2), R = R2>> {
        CWrapper(Reduce {
            inner: self.0,
            input_maps: HashMap::new(),
            output_maps: HashMap::new(),
            proc,
            phantom: PhantomData,
        })
    }
}

impl<D: Key, R: Monoid, C: Operator<D = D, R = R>> Arrangement<D, R, C> {
    fn flow<'a>(&'a mut self, step: usize) {
        let Arrangement {
            ref mut from,
            ref mut value,
        } = self;
        from.flow(step, |x, r| value.add(x, r));
    }
}

struct Input<D, R> {
    current_step: usize,
    pending: HashMap<D, R>,
}

impl<D: Key, R: Monoid> DynOperator for Input<D, R> {
    type D = D;
    type R = R;
    fn flow_to(&mut self, step: usize) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}

impl<D: Key, R: Monoid> Operator for Input<D, R> {
    fn flow<F: FnMut(D, R)>(&mut self, step: usize, mut send: F) {
        let xs = if step > self.current_step {
            mem::take(&mut self.pending)
        } else {
            return;
        };
        self.current_step = step;
        for (x, r) in xs {
            send(x, r);
        }
    }
}

struct FlatMap<C, MF> {
    inner: C,
    op: MF,
}

impl<
        D1,
        R1,
        D2: Key,
        R2: Monoid,
        C: Operator<D = D1, R = R1>,
        I: IntoIterator<Item = (D2, R2)>,
        MF: Fn(D1, R1) -> I,
    > DynOperator for FlatMap<C, MF>
{
    type D = D2;
    type R = R2;
    fn flow_to(&mut self, step: usize) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}

impl<
        D1,
        R1,
        D2: Key,
        R2: Monoid,
        C: Operator<D = D1, R = R1>,
        I: IntoIterator<Item = (D2, R2)>,
        MF: Fn(D1, R1) -> I,
    > Operator for FlatMap<C, MF>
{
    fn flow<F: FnMut(D2, R2)>(&mut self, step: usize, mut send: F) {
        let FlatMap {
            ref mut inner,
            ref op,
        } = self;
        inner.flow(step, |x, r| {
            for (x2, r2) in op(x, r) {
                send(x2, r2)
            }
        })
    }
}

struct Concat<C1, C2> {
    left: C1,
    right: C2,
}

impl<D: Key, R: Monoid, C1: Operator<D = D, R = R>, C2: Operator<D = D, R = R>> DynOperator
    for Concat<C1, C2>
{
    type D = D;
    type R = R;
    fn flow_to(&mut self, step: usize) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}

impl<D: Key, R: Monoid, C1: Operator<D = D, R = R>, C2: Operator<D = D, R = R>> Operator
    for Concat<C1, C2>
{
    fn flow<F: FnMut(D, R)>(&mut self, step: usize, mut send: F) {
        self.left.flow(step, &mut send);
        self.right.flow(step, send);
    }
}

struct Source<C: Operator> {
    source: C,
    listeners: Vec<Weak<RefCell<HashMap<C::D, C::R>>>>,
    step: usize,
}

pub struct Receiver<C: Operator> {
    data: Rc<RefCell<HashMap<C::D, C::R>>>,
    source: Rc<RefCell<Source<C>>>,
}

impl<C: Operator> Clone for Receiver<C> {
    fn clone(&self) -> Self {
        let data = Rc::new(RefCell::new(self.data.borrow().clone()));
        self.source
            .borrow_mut()
            .listeners
            .push(Rc::downgrade(&data));
        Receiver {
            data,
            source: self.source.clone(),
        }
    }
}

impl<C: Operator> DynOperator for Receiver<C> {
    type D = C::D;
    type R = C::R;
    fn flow_to(&mut self, step: usize) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}

impl<C: Operator> Operator for Receiver<C> {
    fn flow<F: FnMut(C::D, C::R)>(&mut self, step: usize, mut send: F) {
        if self.source.borrow().step < step {
            let mut source = self.source.borrow_mut();
            let Source {
                source: ref mut inner,
                ref listeners,
                step: ref mut prev_step,
            } = &mut *source;
            *prev_step = step;
            let mut changes = HashMap::new();
            inner.flow(step, |x, r| changes.add(x, r));
            for (listener, changes) in listeners.iter().tuple_with(changes) {
                if let Some(l) = listener.upgrade() {
                    let mut lborrowed = l.borrow_mut();
                    for (x, r) in changes {
                        lborrowed.add(x, r);
                    }
                }
            }
        }
        for (x, r) in mem::take(&mut *self.data.borrow_mut()) {
            send(x, r)
        }
    }
}

struct Join<LC, RC, K, LD, LR, RD, RR> {
    left: LC,
    right: RC,
    left_map: HashMap<K, HashMap<LD, LR>>,
    right_map: HashMap<K, HashMap<RD, RR>>,
}

impl<
        LC: Operator<D = (K, LD), R = LR>,
        RC: Operator<D = (K, RD), R = RR>,
        K: Key,
        LD: Key,
        LR: Monoid + Mul<RR, Output = OR>,
        RD: Key,
        RR: Monoid,
        OR: Monoid,
    > DynOperator for Join<LC, RC, K, LD, LR, RD, RR>
{
    type D = (K, LD, RD);
    type R = OR;
    fn flow_to(&mut self, step: usize) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}
impl<
        LC: Operator<D = (K, LD), R = LR>,
        RC: Operator<D = (K, RD), R = RR>,
        K: Key,
        LD: Key,
        LR: Monoid + Mul<RR, Output = OR>,
        RD: Key,
        RR: Monoid,
        OR: Monoid,
    > Operator for Join<LC, RC, K, LD, LR, RD, RR>
{
    fn flow<F: FnMut(Self::D, Self::R)>(&mut self, step: usize, mut send: F) {
        let Join {
            left,
            right,
            left_map,
            right_map,
        } = self;
        left.flow(step, |(k, lx), lr| {
            for (rx, rr) in right_map.get(&k).borrow_or_default().iter() {
                send((k.clone(), lx.clone(), rx.clone()), lr.clone() * rr.clone());
            }
            left_map.add((k, lx), lr);
        });
        right.flow(step, |(k, rx), rr| {
            for (lx, lr) in left_map.get(&k).borrow_or_default().iter() {
                send((k.clone(), lx.clone(), rx.clone()), lr.clone() * rr.clone());
            }
            right_map.add((k, rx), rr);
        });
    }
}

struct Reduce<D2, R2, C, K, M1, M2, F: Fn(K, &M1) -> M2> {
    inner: C,
    input_maps: HashMap<K, M1>,
    output_maps: HashMap<K, M2>,
    proc: F,
    phantom: PhantomData<(D2, R2)>,
}

impl<
        C: Operator<D = (K, D1)>,
        K: Key,
        D1,
        M1: IsAddMap<D1, C::R>,
        M2: IsMap<D2, R2>,
        MF: Fn(K, &M1) -> M2,
        D2: Key,
        R2: Monoid,
    > DynOperator for Reduce<D2, R2, C, K, M1, M2, MF>
{
    type D = (K, D2);
    type R = R2;
    fn flow_to(&mut self, step: usize) -> HashMap<Self::D, Self::R> {
        default_flow_to(self, step)
    }
}

impl<
        C: Operator<D = (K, D1)>,
        K: Key,
        D1,
        M1: IsAddMap<D1, C::R>,
        M2: IsMap<D2, R2>,
        MF: Fn(K, &M1) -> M2,
        D2: Key,
        R2: Monoid,
    > Operator for Reduce<D2, R2, C, K, M1, M2, MF>
{
    fn flow<F: FnMut((K, D2), R2)>(&mut self, step: usize, mut send: F) {
        let mut changed_keys = HashSet::new();
        let Reduce {
            inner, input_maps, ..
        } = self;
        inner.flow(step, |(k, x), r| {
            changed_keys.insert(k.clone());
            input_maps.add((k, x), r);
        });
        for k in changed_keys {
            let old_map = match self.input_maps.get(&k) {
                None => {
                    if let Some(om) = self.output_maps.remove(&k) {
                        om.into_discardable()
                    } else {
                        M2::Discardable::default()
                    }
                }
                Some(im) => {
                    let new_map = (self.proc)(k.clone(), im);
                    let e = self.output_maps.entry(k.clone());
                    let (mut old_map, new_map_ref) = match e {
                        hash_map::Entry::Vacant(ve) => {
                            let new_map_ref: &M2 = ve.insert(new_map);
                            (M2::Discardable::default(), new_map_ref)
                        }
                        hash_map::Entry::Occupied(oe) => {
                            let new_map_ref = oe.into_mut();
                            (
                                mem::replace(new_map_ref, new_map).into_discardable(),
                                &*new_map_ref,
                            )
                        }
                    };
                    new_map_ref.foreach(|x, r| {
                        let or = old_map.remove(x).unwrap_or_default();
                        let diff = r.clone() - or;
                        if !diff.is_zero() {
                            send((k.clone(), x.clone()), diff)
                        }
                    });
                    old_map
                }
            };
            for (x, r) in old_map.into_iter() {
                send((k.clone(), x), -r)
            }
        }
    }
}

pub struct WCollection<D, R>(Box<dyn DynOperator<D = D, R = R>>);

impl<D: Key, R: Monoid> DynOperator for WCollection<D, R> {
    type D = D;
    type R = R;
    fn flow_to(&mut self, step: usize) -> HashMap<Self::D, Self::R> {
        self.0.flow_to(step)
    }
}

impl<D: Key, R: Monoid> Operator for WCollection<D, R> {
    fn flow<F: FnMut(D, R)>(&mut self, step: usize, mut send: F) {
        let res = self.0.flow_to(step);
        for (x, r) in res {
            send(x, r)
        }
    }
}

#[cfg(test)]
mod tests;
