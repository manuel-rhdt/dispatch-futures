extern crate dispatch_futures;
extern crate futures;
extern crate reqwest;
extern crate pbr;

use dispatch_futures::*;
use futures::sync::oneshot::spawn_fn;
use futures::sync::mpsc::channel;
use futures::{Future, Stream, Sink};
use reqwest::header::ContentLength;
use pbr::{ProgressBar, Units};

use std::thread;
use std::time::Duration;
use std::io;
use std::io::prelude::*;

fn main() {

    let executor = DispatchQueue::default();

    let (mut tx, rx) = channel(8);

    let a = spawn_fn(
        move || {
            let mut resp = reqwest::get("http://ipv4.download.thinkbroadband.com/50MB.zip").unwrap();
            assert!(resp.status().is_success());

            let &ContentLength(total_length) = resp.headers().get().unwrap();

            let mut buffer = [0u8; 64*1024];
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
        },
        &executor,
    );

    let mut pb = ProgressBar::new(10000);
    pb.set_units(Units::Bytes);

    let result = rx.for_each(|(veclen, total)| {
        pb.total = total;
        pb.set(veclen as u64);
        Ok(())
    }).map_err(|_| io::Error::new(io::ErrorKind::Other, "oh no!")).join(a).wait();

    result.unwrap();
    pb.finish_print("done");
    // println!("result {:?}", result);
}