use super::Op;
use crate::core::borrow::BorrowOrDefault;
use crate::core::is_map::IsAddMap;
use crate::core::key::Key;
use crate::core::monoid::Monoid;
use crate::core::node::Node;
use crate::core::{Relation, Step};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Mul;

struct BiMap<A, B, C: Op<D = (A, B)>> {
    forward: HashMap<A, HashMap<B, C::R>>,
    backward: HashMap<B, HashMap<A, C::R>>,
    inner: Node<C>,
}

impl<A, B, C: Op<D = (A, B)>> BiMap<A, B, C> {
    fn new(inner: Node<C>) -> Self {
        BiMap {
            forward: HashMap::new(),
            backward: HashMap::new(),
            inner,
        }
    }
}

impl<A: Key, B: Key, C: Op<D = (A, B)>> BiMap<A, B, C>
where
    C::R: Mul<C::R, Output = C::R>,
{
    fn flow<F: FnMut((A, B), C::R)>(&mut self, step: &Step, mut send: F) {
        let BiMap {
            forward,
            backward,
            inner,
        } = self;
        inner.flow(step, |(x, y), r| {
            forward.add((x.clone(), y.clone()), r.clone());
            backward.add((y.clone(), x.clone()), r.clone());
            send((x, y), r)
        })
    }
}

struct Triangles<
    X: Key,
    Y: Key,
    Z: Key,
    R: Monoid + Mul<R, Output = R>,
    C1: Op<D = (X, Y), R = R>,
    C2: Op<D = (X, Z), R = R>,
    C3: Op<D = (Y, Z), R = R>,
> {
    mxy: BiMap<X, Y, C1>,
    mxz: BiMap<X, Z, C2>,
    myz: BiMap<Y, Z, C3>,
}

impl<
        X: Key,
        Y: Key,
        Z: Key,
        R: Monoid + Mul<R, Output = R>,
        C1: Op<D = (X, Y), R = R>,
        C2: Op<D = (X, Z), R = R>,
        C3: Op<D = (Y, Z), R = R>,
    > Op for Triangles<X, Y, Z, R, C1, C2, C3>
{
    type D = (X, Y, Z);
    type R = R;

    fn default_op_name() -> &'static str {
        "triangles"
    }
    fn flow<F: FnMut((X, Y, Z), R)>(&mut self, step: &Step, mut send: F) {
        let Triangles { mxy, mxz, myz } = self;
        mxy.flow(step, |(x, y), rxy| {
            for (z, (rxz, ryz)) in intersection(
                &mxz.forward.get(&x).borrow_or_default(),
                &myz.forward.get(&y).borrow_or_default(),
            ) {
                send(
                    (x.clone(), y.clone(), z.clone()),
                    rxy.clone() * rxz.clone() * ryz.clone(),
                )
            }
        });
        mxz.flow(step, |(x, z), rxz| {
            for (y, (rxy, ryz)) in intersection(
                &mxy.forward.get(&x).borrow_or_default(),
                &myz.backward.get(&z).borrow_or_default(),
            ) {
                send(
                    (x.clone(), y.clone(), z.clone()),
                    rxy.clone() * rxz.clone() * ryz.clone(),
                )
            }
        });
        myz.flow(step, |(y, z), ryz| {
            for (x, (rxy, rxz)) in intersection(
                &mxy.backward.get(&y).borrow_or_default(),
                &mxz.backward.get(&z).borrow_or_default(),
            ) {
                send(
                    (x.clone(), y.clone(), z.clone()),
                    rxy.clone() * rxz.clone() * ryz.clone(),
                )
            }
        });
    }
}

fn intersection<K: Clone + Eq + Hash, V1: Clone, V2: Clone>(
    l: &HashMap<K, V1>,
    r: &HashMap<K, V2>,
) -> HashMap<K, (V1, V2)> {
    if r.len() < l.len() {
        r.iter()
            .flat_map(|(k, v2)| {
                if let Some(v1) = l.get(k) {
                    Some((k.clone(), (v1.clone(), v2.clone())))
                } else {
                    None
                }
            })
            .collect()
    } else {
        l.iter()
            .flat_map(|(k, v1)| {
                if let Some(v2) = r.get(k) {
                    Some((k.clone(), (v1.clone(), v2.clone())))
                } else {
                    None
                }
            })
            .collect()
    }
}

impl<'a, X: Key, Y: Key, R: Monoid + Mul<R, Output = R>, C1: Op<D = (X, Y), R = R>>
    Relation<'a, C1>
{
    /// Find all triangles in a tri-partite graph
    pub fn triangles<Z: Key, C2: Op<D = (X, Z), R = R>, C3: Op<D = (Y, Z), R = R>>(
        self,
        r2: Relation<'a, C2>,
        r3: Relation<'a, C3>,
    ) -> Relation<'a, impl Op<D = (X, Y, Z), R = R>> {
        Relation::new(
            vec![self.dep(), r2.dep(), r3.dep()],
            Triangles {
                mxy: BiMap::new(self.inner),
                mxz: BiMap::new(r2.inner),
                myz: BiMap::new(r3.inner),
            },
            self.node_maker,
        )
    }
}
