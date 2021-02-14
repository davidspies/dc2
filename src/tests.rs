use crate::key::Key;
use crate::Collection;
use crate::CreationContext;
use std::collections::HashMap;

#[test]
fn it_works() {
    let creation = CreationContext::new();
    let (sender, v) = creation.create_input::<usize, isize>();
    let c = v.map(|x: usize| x + 1);
    let outp = c.get_arrangement(&creation);
    let mut execution = creation.begin();
    sender.insert(&execution, 1);
    sender.insert(&execution, 2);
    sender.insert(&execution, 3);
    assert_eq!(&*outp.read(&execution), &HashMap::new());
    assert_eq!(&*outp.read(&execution), &HashMap::new());
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![(2, 1), (3, 1), (4, 1)].into_iter().collect()
    );
    sender.delete(&execution, 2);
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![(2, 1), (4, 1)].into_iter().collect()
    );
}

fn transitive_closure_acyclic<V: Key>(
    edges: Collection<'static, (V, V)>,
    creation: &mut CreationContext,
) -> Collection<'static, (V, V)> {
    distances_gen(edges, creation).map(|(e, ())| e).collect()
}

fn distances<V: Key>(
    edges: Collection<'static, (V, V)>,
    creation: &mut CreationContext,
) -> Collection<'static, ((V, V), usize)> {
    distances_gen(edges, creation)
}

trait Distance: Key + Ord {
    fn increment(self) -> Self;
    fn one() -> Self;
}

impl Distance for usize {
    fn increment(self) -> Self {
        self + 1
    }
    fn one() -> Self {
        1
    }
}

impl Distance for () {
    fn increment(self) -> Self {
        ()
    }
    fn one() -> Self {
        ()
    }
}

fn distances_gen<V: Key, T: Distance>(
    edges: Collection<'static, (V, V)>,
    creation: &mut CreationContext,
) -> Collection<'static, ((V, V), T)> {
    let mut subcontext = creation.subgraph::<T>();
    let (closure_var, closure) = subcontext.variable::<(V, V), isize>();
    let closure_prev = closure.map(|(d, e)| (e, d));
    let closure_n = edges
        .clone()
        .map(|e| (e, Distance::one()))
        .concat(
            closure_prev
                .map(|((l, r), d)| (r, (l, d)))
                .join(edges)
                .map(|(_, (l, d), r)| ((l, r), d.increment())),
        )
        .group_min()
        .split();
    closure_var.set(closure_n.clone().map(|(e, d)| (d, e)));
    closure_n.leave(&subcontext.finish()).collect()
}

#[test]
fn test_transitive_closure() {
    let mut creation = CreationContext::new();
    let (edge_input, edges) = creation.create_input::<(char, char), isize>();
    let res = transitive_closure_acyclic(edges.collect(), &mut creation);
    let outp = res.get_arrangement(&creation);
    let mut execution = creation.begin();
    edge_input.insert(&execution, ('A', 'B'));
    edge_input.insert(&execution, ('B', 'C'));
    edge_input.insert(&execution, ('A', 'C'));
    edge_input.insert(&execution, ('D', 'E'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            (('A', 'B'), 1),
            (('B', 'C'), 1),
            (('A', 'C'), 1),
            (('D', 'E'), 1)
        ]
        .into_iter()
        .collect()
    );
    edge_input.insert(&execution, ('C', 'D'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            (('C', 'D'), 1),
            (('A', 'D'), 1),
            (('B', 'D'), 1),
            (('C', 'E'), 1),
            (('A', 'E'), 1),
            (('B', 'E'), 1),
            (('A', 'B'), 1),
            (('B', 'C'), 1),
            (('A', 'C'), 1),
            (('D', 'E'), 1)
        ]
        .into_iter()
        .collect()
    );
    edge_input.delete(&execution, ('A', 'C'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            (('C', 'D'), 1),
            (('A', 'D'), 1),
            (('B', 'D'), 1),
            (('C', 'E'), 1),
            (('A', 'E'), 1),
            (('B', 'E'), 1),
            (('A', 'B'), 1),
            (('B', 'C'), 1),
            (('A', 'C'), 1),
            (('D', 'E'), 1)
        ]
        .into_iter()
        .collect()
    );
    edge_input.delete(&execution, ('A', 'B'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            (('C', 'D'), 1),
            (('B', 'D'), 1),
            (('C', 'E'), 1),
            (('B', 'E'), 1),
            (('B', 'C'), 1),
            (('D', 'E'), 1)
        ]
        .into_iter()
        .collect()
    );
}

#[test]
fn test_transitive_closure_distances() {
    let mut creation = CreationContext::new();
    let (edge_input, edges) = creation.create_input::<(char, char), isize>();
    let res = distances(edges.collect(), &mut creation);
    let outp = res.get_arrangement(&creation);
    let mut execution = creation.begin();
    edge_input.insert(&execution, ('A', 'B'));
    edge_input.insert(&execution, ('B', 'C'));
    edge_input.insert(&execution, ('A', 'C'));
    edge_input.insert(&execution, ('D', 'E'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            ((('A', 'B'), 1), 1),
            ((('B', 'C'), 1), 1),
            ((('A', 'C'), 1), 1),
            ((('D', 'E'), 1), 1)
        ]
        .into_iter()
        .collect()
    );
    edge_input.insert(&execution, ('C', 'D'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            ((('C', 'D'), 1), 1),
            ((('A', 'D'), 2), 1),
            ((('B', 'D'), 2), 1),
            ((('C', 'E'), 2), 1),
            ((('A', 'E'), 3), 1),
            ((('B', 'E'), 3), 1),
            ((('A', 'B'), 1), 1),
            ((('B', 'C'), 1), 1),
            ((('A', 'C'), 1), 1),
            ((('D', 'E'), 1), 1)
        ]
        .into_iter()
        .collect()
    );
    edge_input.delete(&execution, ('A', 'C'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            ((('A', 'C'), 2), 1),
            ((('A', 'D'), 3), 1),
            ((('A', 'E'), 4), 1),
            ((('C', 'D'), 1), 1),
            ((('B', 'D'), 2), 1),
            ((('C', 'E'), 2), 1),
            ((('B', 'E'), 3), 1),
            ((('A', 'B'), 1), 1),
            ((('B', 'C'), 1), 1),
            ((('D', 'E'), 1), 1)
        ]
        .into_iter()
        .collect()
    );
}

#[test]
fn test_transitive_closure_cyclic() {
    let mut creation = CreationContext::new();
    let (edge_input, edges) = creation.create_input::<(char, char), isize>();
    let res = distances(edges.collect(), &mut creation);
    let outp = res.get_arrangement(&creation);
    let mut execution = creation.begin();
    edge_input.insert(&execution, ('A', 'B'));
    edge_input.insert(&execution, ('B', 'C'));
    edge_input.insert(&execution, ('C', 'A'));
    edge_input.insert(&execution, ('D', 'E'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            ((('A', 'B'), 1), 1),
            ((('B', 'C'), 1), 1),
            ((('C', 'A'), 1), 1),
            ((('A', 'C'), 2), 1),
            ((('B', 'A'), 2), 1),
            ((('C', 'B'), 2), 1),
            ((('A', 'A'), 3), 1),
            ((('B', 'B'), 3), 1),
            ((('C', 'C'), 3), 1),
            ((('D', 'E'), 1), 1)
        ]
        .into_iter()
        .collect()
    );
    edge_input.insert(&execution, ('C', 'D'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            ((('C', 'D'), 1), 1),
            ((('B', 'D'), 2), 1),
            ((('A', 'D'), 3), 1),
            ((('C', 'E'), 2), 1),
            ((('B', 'E'), 3), 1),
            ((('A', 'E'), 4), 1),
            ((('A', 'B'), 1), 1),
            ((('B', 'C'), 1), 1),
            ((('C', 'A'), 1), 1),
            ((('A', 'C'), 2), 1),
            ((('B', 'A'), 2), 1),
            ((('C', 'B'), 2), 1),
            ((('A', 'A'), 3), 1),
            ((('B', 'B'), 3), 1),
            ((('C', 'C'), 3), 1),
            ((('D', 'E'), 1), 1)
        ]
        .into_iter()
        .collect()
    );
    edge_input.delete(&execution, ('C', 'A'));
    execution.commit();
    assert_eq!(
        &*outp.read(&execution),
        &vec![
            ((('C', 'D'), 1), 1),
            ((('B', 'D'), 2), 1),
            ((('A', 'D'), 3), 1),
            ((('C', 'E'), 2), 1),
            ((('B', 'E'), 3), 1),
            ((('A', 'E'), 4), 1),
            ((('A', 'B'), 1), 1),
            ((('B', 'C'), 1), 1),
            ((('A', 'C'), 2), 1),
            ((('D', 'E'), 1), 1)
        ]
        .into_iter()
        .collect()
    );
}
