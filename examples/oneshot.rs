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

#![feature(alloc_system)]
extern crate futures;
extern crate futures_libdispatch;
extern crate env_logger;

use futures::prelude::*;
use futures_libdispatch::QueueExecutor;

use std::time::Duration;

use futures::sync::oneshot;

use std::thread::sleep;

fn main() {
    env_logger::init().unwrap();

    loop {
        let executor = QueueExecutor::default();

        let (tx, rx) = oneshot::channel();

        let future = executor.spawn_fn(|| {
            println!("waiting for sender");
            rx
        });

        executor.async(|| {
            sleep(Duration::from_secs(1));
            tx.send(5).unwrap();
        });

        assert_eq!(future.wait().unwrap(), 5);
        println!("got result")
    }
}