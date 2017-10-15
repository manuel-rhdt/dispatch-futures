//    Copyright 2017 Manuel Reinhardt
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

extern crate futures;
extern crate futures_libdispatch;
extern crate tokio_timer;
extern crate env_logger;

use futures::prelude::*;
use futures_libdispatch::QueueExecutor;
use tokio_timer::Timer;

use std::time::Duration;

#[inline(never)]
fn inner_func() {
    let timer = Timer::default();

    let future = timer.sleep(Duration::from_secs(1));
    let future = future.and_then(|_| {
        println!("slept for 1 sec");
        Ok(())
    });
    let future = future.and_then(move |_| timer.sleep(Duration::from_secs(1)));
    let future = future.and_then(|_| {
        println!("slept for 2 sec");
        Ok(())
    });

    let executor = QueueExecutor::default();

    let dispatch_fut = executor.spawn_fn(move || {
        println!("start sleeping...");
        future
    });

    dispatch_fut.wait().unwrap();
}

#[test]
fn timer_test() {
    inner_func();
    println!("finished");
}

use futures::sync::oneshot;

#[test]
fn oneshot_test() {
    let executor = QueueExecutor::default();

    let (tx, rx) = oneshot::channel();

    let future = executor.spawn_fn(|| {
        rx
    });

    executor.spawn_fn(|| {
        tx.send(5)
    });

    assert_eq!(future.wait().unwrap(), 5);
}