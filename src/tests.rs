use crate::{Arrangement, CreationContext};
use std::collections::HashMap;

#[test]
fn it_works() {
    let creation = CreationContext::new();
    let (sender, v) = creation.create_input::<usize, isize>();
    let c = v.map(|x: usize| x + 1);
    let outp = c.get_arrangement::<HashMap<_, _>>(&creation);
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

#[test]
fn test_readme() {
    let creation = CreationContext::new();

    let (input1, relation1) = creation.create_input::<(char, usize), _>();
    let (input2, relation2) = creation.create_input::<(char, String), _>();
    let foo = relation2.split();
    let bar = relation1.join(foo.clone());
    let baz = foo
        .clone()
        .map(|(_, s)| (s.as_str().chars().next().unwrap_or('x'), s.len()));
    let qux = bar
        .map(|(c, (n, s))| (c, n + s.len()))
        .concat(baz)
        .distinct();
    let arrangement: Arrangement<(char, usize)> = qux.get_dyn_arrangement(&creation);

    let mut context = creation.begin();

    input1.insert(&context, ('a', 5));
    input1.insert(&context, ('b', 6));
    input2.insert(&context, ('b', "Hello".to_string()));
    input2.insert(&context, ('b', "world".to_string()));
    context.commit();

    assert_eq!(
        &*arrangement.read(&context),
        &vec![(('H', 5), 1), (('b', 11), 1), (('w', 5), 1)]
            .into_iter()
            .collect()
    );

    input1.delete(&context, ('b', 6));
    input2.insert(&context, ('a', "Goodbye".to_string()));
    context.commit();

    assert_eq!(
        &*arrangement.read(&context),
        &vec![(('G', 7), 1), (('H', 5), 1), (('a', 12), 1), (('w', 5), 1)]
            .into_iter()
            .collect()
    );
}
