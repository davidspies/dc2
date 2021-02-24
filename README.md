# Standing Relations
This crate provides an interface vaguely similar to differential-dataflow for creating standing relations over a shifting dataset.
Critically, _unlike_ differential-dataflow the operators here are single-threaded and optimized for fast turnaround rather than high throughput. That is, this package is intended to be used in "feedback loop" scenarios where the calling code uses the output to determine which inputs to feed in next.

To get started, create a `CreationContext`:

```rust
use dc2::CreationContext;

let mut creation = CreationContext::new();
```

And some inputs:

```rust
let (input1, relation1) = creation.create_input::<(char, usize), _>();
let (input2, relation2) = creation.create_input::<(char, String), _>();
```

Set up your relational operations:

```rust
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
```

Begin inserting data:

```rust
let mut context = creation.begin();

input1.insert(&context, ('a', 5));
input1.insert(&context, ('b', 6));
input2.insert(&context, ('b', "Hello".to_string()));
input2.insert(&context, ('b', "world".to_string()));
```

Commit your changes:

```rust
context.commit();
```

Read the output:

```rust
assert_eq!(
    &*arrangement.read(&context),
    &vec![(('H', 5), 1), (('b', 11), 1), (('w', 5), 1)]
        .into_iter()
        .collect()
);
```

Make some changes (and commit them):

```rust
input1.delete(&context, ('b', 6));
input2.insert(&context, ('a', "Goodbye".to_string()));
context.commit();
```

Read the new output:
```rust
assert_eq!(
    &*arrangement.read(&context),
    &vec![(('G', 7), 1), (('H', 5), 1), (('a', 12), 1), (('w', 5), 1)]
        .into_iter()
        .collect()
);
```

If the compiler is complaining, running slowly, or using too much memory, consider using
`relation.dynamic()` to simplify your type signatures.
