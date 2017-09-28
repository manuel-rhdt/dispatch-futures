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

// This example downloads from a URL to memory and asynchronously while displaying a progress bar on
// the main thread.
extern crate futures_libdispatch;
extern crate futures;
extern crate reqwest;
extern crate pbr;
extern crate env_logger;

use futures_libdispatch::*;
use futures::sync::mpsc::channel;
use futures::{Future, Stream, Sink};
use reqwest::header::ContentLength;
use pbr::{ProgressBar, Units};

use std::io;
use std::io::prelude::*;
use std::env;

fn main() {
    env_logger::init().unwrap();
    let mut args = env::args();
    if args.len() != 2 {
        panic!("Needs exactly one argument specifying the URL from which to download.");
    }
    let url = args.nth(1).unwrap();

    let executor = QueueExecutor::default();

    let (mut tx, rx) = channel(8);
    let a = executor.spawn_fn(move || {
        let mut resp = reqwest::get(&url).unwrap();
        assert!(resp.status().is_success());

        let &ContentLength(total_length) = resp.headers().get().expect(
            "Missing content length. Try another URL.",
        );

        let mut buffer = [0u8; 128 * 1024];
        let mut vector = Vec::new();

        loop {
            let len = match resp.read(&mut buffer) {
                Ok(0) => break,
                Ok(len) => len,
                Err(ref err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            };
            vector.extend_from_slice(&buffer[0..len]);
            match tx.send((vector.len(), total_length)).wait() {
                Ok(new_tx) => tx = new_tx,
                Err(_) => panic!("Oh no!"),
            }
        }

        Ok(vector)
    });

    let mut pb = ProgressBar::new(10000);
    pb.set_units(Units::Bytes);

    let result = rx.for_each(|(veclen, total)| {
        pb.total = total;
        pb.set(veclen as u64);
        Ok(())
    }).map_err(|_| io::Error::new(io::ErrorKind::Other, "oh no!"))
        .join(a)
        .wait();

    result.unwrap();
    pb.finish_print("done");
}
