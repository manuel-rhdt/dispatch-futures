Futures-Executor based on libdispatch
=====================================

This crate contains an implementation of an executor for futures that will schedule them on
the dispatch queues provided by libdispatch. It builds upon the safe wrappers provided by the
`dispatch` crate.

This crate can be used in various ways. You may just want to speed up some complex computation
by identifying its independent parts and scheduling them on separate futures to make optimal use
of the multithreading capacities of modern computers.

The crate is not well tested and there is currently no support for event sources. Not
recommended for serious use just yet!

Examples
--------

Run two functions in parallel on a concurrent queue and wait for them to finish:

```
extern crate futures_libdispatch;
extern crate futures;

use futures_libdispatch::QueueExecutor;
use futures::prelude::*;

#[derive(Debug)]
struct Error;

fn expensive_calculation(input: f64) -> Result<f64, Error> {
    // perform expensive work
#    Ok(input)
}

fn main() {
    // creates a executor with the default concurrent queue
    let executor = QueueExecutor::default();

    let first_calculation = executor.spawn_fn(|| expensive_calculation(1.0));
    let second_calculation = executor.spawn_fn(|| expensive_calculation(2.0));

    // combine futures
    let combined_calculations = first_calculation.join(second_calculation);
    let (result1, result2) = combined_calculations.wait().unwrap();
    # assert_eq!(result1, 1.0);
    # assert_eq!(result2, 2.0);
}
```
