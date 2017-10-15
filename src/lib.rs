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


//! Futures-Executor based on libdispatch
//! =====================================
//!
//! This crate contains an implementation of an executor for futures that will schedule them on
//! the dispatch queues provided by libdispatch. It builds upon the safe wrappers provided by the
//! `dispatch` crate.
//!
//! This crate can be used in various ways. You may just want to speed up some complex computation
//! by identifying its independent parts and scheduling them on separate futures to make optimal use
//! of the multithreading capacities of modern computers.
//!
//! The crate is not well tested and there is currently no support for event sources. Not
//! recommended for serious use just yet!
//!
//! Examples
//! --------
//!
//! Run two functions in parallel on a concurrent queue and wait for them to finish:
//!
//! ```
//! extern crate futures_libdispatch;
//! extern crate futures;
//!
//! use futures_libdispatch::QueueExecutor;
//! use futures::prelude::*;
//!
//! #[derive(Debug)]
//! struct Error;
//!
//! fn expensive_calculation(input: f64) -> Result<f64, Error> {
//!     // perform expensive work
//! #    Ok(input)
//! }
//!
//! fn main() {
//!     // creates a executor with the default concurrent queue
//!     let executor = QueueExecutor::default();
//!
//!     let first_calculation = executor.spawn_fn(|| expensive_calculation(1.0));
//!     let second_calculation = executor.spawn_fn(|| expensive_calculation(2.0));
//!
//!     // combine futures
//!     let combined_calculations = first_calculation.join(second_calculation);
//!     let (result1, result2) = combined_calculations.wait().unwrap();
//!     # assert_eq!(result1, 1.0);
//!     # assert_eq!(result2, 2.0);
//! }
//! ```
//!

extern crate dispatch;
extern crate futures;
#[macro_use]
extern crate log;

use futures::{Async, Future};
use futures::future::{Executor, ExecuteError, IntoFuture};
use futures::executor::{self, Notify};
use futures::sync::oneshot::{spawn, spawn_fn, SpawnHandle};

use std::fmt;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::*;
use std::ops::Deref;

/// A future acting as a proxy to the original future passed to `QueueExecutor::spawn`.
///
/// A `EnqueuedFuture` represents a calculation performed on a dispatch queue.
/// If the `EnqueuedFuture` is dropped before it completed its execution on the gcd queue will tried
/// to be canceled. If you want to have the future execute in the background and discarding its
/// result see the `forget()` method.
///
/// Examples
/// --------
///
/// You can `.wait()` on a `EnqueuedFuture` to block until the computation finishes.
/// You have to be careful however that if your code is currently executing in a serial queue you
/// shouldn't call `.wait()` on a future spawned on the same serial queue as this will deadlock the
/// queue.
///
/// ```
/// # extern crate futures_libdispatch;
/// # extern crate futures;
///
/// use futures_libdispatch::QueueExecutor;
/// use futures::Future;
///
/// # fn main() {
/// let executor = QueueExecutor::default();
/// let future = executor.spawn_fn(|| {
/// #   let result = 0;
///     // long computation...
///     Ok(result) as Result<i32, ()>
/// });
///
/// // This will not return until the execution of the closure has completed
/// // (on another thread)
/// let result = future.wait().unwrap();
/// # assert_eq!(result, 0);
/// # }
/// ```
///
#[derive(Debug)]
pub struct EnqueuedFuture<T, E> {
    inner: SpawnHandle<T, E>,
    queue_inner: Arc<Inner>,
}

impl<T, E> EnqueuedFuture<T, E> {
    /// Forget the future, i.e. run it in the background and discard its result.
    ///
    /// The future will be polled until it has completed and its result
    /// will be ignored regardless if the future execution was successful or not. This is usually
    /// useful for logging and other unimportant tasks that need to be scheduled asynchronously.
    pub fn forget(self) {
        self.inner.forget()
    }

    /// Returns the queue onto which the contained future was enqueued.
    pub fn queue(&self) -> QueueExecutor {
        QueueExecutor { inner: Arc::clone(&self.queue_inner) }
    }
}

impl<T: Send, E> Future for EnqueuedFuture<T, E> {
    type Item = T;
    type Error = E;
    fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

/// A Wrapper of a dispatch queue that can execute futures.
///
/// It is created from a wrapped representation of a dispatch queue provided by the `dispatch`
/// crate. The executor allows you to run arbitrary futures on the provided queue and thus
/// automatically benefits from all available CPU cores.
///
/// After creation of the `QueueExecutor` one can still access the original queue using
/// `QueueExecutor`'s deref implementation. This means you can still queue up work on the Queue
/// using the API from the `dispatch` crate.
///
/// Examples
/// --------
///
/// Create an executor from the global parallel queue with default priority
///
/// ```
/// use futures_libdispatch::QueueExecutor;
/// let executor = QueueExecutor::default();
/// ```
///
/// and use it to spawn some Futures.
///
/// ```rust,ignore
/// let fut1 = executor.spawn(fut1);
/// let fut2 = executor.spawn(fut2);
/// ```
///
#[derive(Clone, Debug)]
pub struct QueueExecutor {
    inner: Arc<Inner>,
}

struct Inner {
    queue: dispatch::Queue,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Queue").field("label", &self.queue.label()).finish()
    }
}

#[derive(Default, Debug)]
struct ScopedFuture<F, SourceT> {
    spawned_fut: Option<executor::Spawn<F>>,
    notify: Option<Arc<Notifier<F, SourceT>>>,
}

impl<F> Future for ScopedFuture<F, dispatch::source::DataOr> where F: Future + Send + 'static {
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> futures::Poll<F::Item, F::Error> {
        // safe because no mutable references of notifier exist
        let notifier = self.notify.as_ref().expect("No notifier!");

        // safe because we know we are the only thread accessing spawned_fut
        let mut future = self.spawned_fut.take().expect("ScopedFuture: polled when future was already complete!");

        match future.poll_future_notify(notifier, 0) {
            result @ Ok(Async::Ready(_)) | result @ Err(_) => {
                trace!("Poll: completed");
                // don't allow any more notifications
                notifier.source.get().map(|source| source.cancel());
                result
            }
            Ok(Async::NotReady) => {
                trace!("Poll: not ready");
                // put future back
                self.spawned_fut = Some(future);
                Ok(Async::NotReady)
            }
        }
    }
}

// state machine can only progress downwards in states.
const UNPOLLED: usize = 0;
const POLLED: usize = 1;
const UPDATING: usize = 2;
const UPDATING_POLLED: usize = 3;
const COMPLETE: usize = 4;

pub struct NotifyLock<Inner> {
    state: AtomicUsize,
    inner: UnsafeCell<Option<Inner>>
}

impl<Inner> NotifyLock<Inner> {
    fn new() -> Self {
        NotifyLock {
            state: AtomicUsize::new(UNPOLLED),
            inner: UnsafeCell::new(None)
        }
    }

    fn get(&self) -> Option<&Inner> {
        if self.state.load(Ordering::Relaxed) == COMPLETE {
            unsafe { (&*self.inner.get()).as_ref() }
        } else {
            None
        }
    }

    fn poll(&self) -> Option<&Inner> {
        loop {
            match self.state.load(Ordering::Relaxed) {
                UNPOLLED => if self.state.compare_and_swap(UNPOLLED, POLLED, Ordering::SeqCst) == UNPOLLED {
                    break;
                },
                UPDATING => if self.state.compare_and_swap(UPDATING, UPDATING_POLLED, Ordering::SeqCst) == UPDATING {
                    break;
                },
                POLLED | UPDATING_POLLED => break,
                COMPLETE => return unsafe { (&*self.inner.get()).as_ref() },
                _ => unreachable!(),
            }
        }
        None
    }

    /// Stores a value in the lock than can be read by subsequent calls to `get`.
    /// Returns true if lock was polled before `set` was called.
    ///
    /// Panics
    /// ------
    /// Panics if called more than once for a lock.

    fn set(&self, value: Inner) -> bool {
        let should_repoll = match self.state.swap(UPDATING, Ordering::Acquire) {
            UPDATING | UPDATING_POLLED | COMPLETE => panic!("NotifyLock: cannot set value more than once!"),
            POLLED => true,
            UNPOLLED => false,
            _ => unreachable!(),
        };
        unsafe { *self.inner.get() = Some(value) };
        match self.state.swap(COMPLETE, Ordering::Release) {
            UPDATING => should_repoll,
            UPDATING_POLLED => true,
            _ => unreachable!(),
        }
    }
}

unsafe impl<T: Send> Send for NotifyLock<T> {}

unsafe impl<T> Sync for NotifyLock<T> {}

pub struct Notifier<F, SourceT> {
    future: UnsafeCell<ScopedFuture<F, SourceT>>,
    source: NotifyLock<dispatch::Source<SourceT>>,
}

impl<F, S> Notifier<F, S> {
    fn new(future: F) -> Arc<Notifier<F, S>> {
        let spawned = executor::spawn(future);
        let scoped = ScopedFuture { spawned_fut: Some(spawned), notify: Default::default() };
        let notifier = Notifier {
            future: UnsafeCell::new(scoped),
            source: NotifyLock::new(),
        };
        let notifier = Arc::new(notifier);

        // Build a reference cycle. Safe because during construction nobody else has access.
        unsafe { (&mut *notifier.future.get()).notify = Some(Arc::clone(&notifier)) };
        notifier
    }

    // No other references to `self` are allowed.
    unsafe fn terminate(&self) {
        trace!("terminate notifier");
        let _ = (&mut *self.future.get()).notify.take();
        let _ = (&mut *self.source.inner.get()).take();
    }
}

impl<F, S> fmt::Debug for Notifier<F, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Notifier").field("state", &self.source.state).finish()
    }
}

impl<F> executor::Notify for Notifier<F, dispatch::source::DataOr> where F: Send {
    fn notify(&self, _: usize) {
        trace!("notify");
        self.source.poll().map(|source| source.merge_data(1));
    }
}

unsafe impl<F, S> Send for Notifier<F, S> where F: Send, S: Send {}

unsafe impl<F, S> Sync for Notifier<F, S> where S: Sync {}

impl<F, S> Drop for Notifier<F, S> {
    fn drop(&mut self) {
        trace!("dropped notifier");
    }
}

impl<F> Executor<F> for QueueExecutor
    where F: Future<Item=(), Error=()> + Send + 'static, {
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        trace!("QueueExecutor::execute");

        let task = Notifier::new(future);

        let target_queue = self.inner.queue.clone();

        self.async(move || {
            // safe mutable access, because task is not yet shared
            match unsafe { (&mut *task.future.get()).poll() } {
                Ok(Async::Ready(())) | Err(()) => {
                    // drop the notifier (mutable access is safe because we never shared the task)
                    unsafe { task.terminate() };
                }
                Ok(Async::NotReady) => {
                    trace!("Create dispatch source");

                    // We have to create a dispatch source now
                    let mut source = dispatch::SourceBuilder::new(dispatch::source::DataOr, &target_queue).unwrap();

                    let shared_task = Arc::clone(&task);
                    // Access to future in the event handler is safe, because the source ensures
                    // that only one handler can run at any given time.
                    source.event_handler(move |_| match unsafe { (&mut *shared_task.future.get()).poll() } {
                        Ok(Async::Ready(())) | Err(()) => {
                            unsafe { shared_task.terminate() };
                        }
                        Ok(Async::NotReady) => {}
                    });


                    let should_renotify = task.source.set(source.resume());
                    if should_renotify {
                        task.notify(0);
                    }
                }
            }
        });

        Ok(())
    }
}

impl QueueExecutor {
    /// Create a new `QueueExecutor` from an existing `queue` wrapped using the `dispatch` crate.
    pub fn new(queue: dispatch::Queue) -> QueueExecutor {
        queue.into()
    }

    /// Immediately submits the provided future to the queue which will then be driven to
    /// completion.
    ///
    /// Returns a Future representing the finished result after the provided future has completed.
    pub fn spawn<F>(&self, future: F) -> EnqueuedFuture<F::Item, F::Error>
        where F: Future + Send + 'static,
              F::Item: Send + 'static,
              F::Error: Send + 'static, {
        EnqueuedFuture {
            inner: spawn(future, self),
            queue_inner: Arc::clone(&self.inner),
        }
    }

    /// Asynchronously runs the provided closure to the queue and returns a Future representing its
    /// result.
    ///
    /// This behaves identical to `executor.spawn(lazy(f))`.
    ///
    /// Note
    /// ----
    /// The provided closure must return a value that implements `IntoFuture`. If you don't actually
    /// create a future in the closure you can just return a `Result` which has a trivial
    /// implementation of `IntoFuture`.
    pub fn spawn_fn<F, R>(&self, f: F) -> EnqueuedFuture<R::Item, R::Error>
        where F: FnOnce() -> R + Send + 'static,
              R: IntoFuture + 'static,
              R::Future: Send + 'static,
              R::Item: Send + 'static,
              R::Error: Send + 'static {
        EnqueuedFuture {
            inner: spawn_fn(f, self),
            queue_inner: Arc::clone(&self.inner),
        }
    }
}

impl Default for QueueExecutor {
    /// Returns the global parallel queue with default priority.

    fn default() -> Self {
        dispatch::Queue::global(dispatch::QueuePriority::Default).into()
    }
}

impl From<dispatch::Queue> for QueueExecutor {
    fn from(queue: dispatch::Queue) -> QueueExecutor {
        let inner = Arc::new(Inner {
            queue,
        });
        QueueExecutor { inner: inner }
    }
}

impl Deref for QueueExecutor {
    type Target = dispatch::Queue;

    fn deref(&self) -> &dispatch::Queue {
        &self.inner.queue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate tokio_timer;

    use self::tokio_timer::*;

    use std::thread;
    use std::time::Duration;

    #[test]
    fn it_works() {
        let executor = QueueExecutor::default();
        let a = executor.spawn_fn(|| {
            for i in 0..10 {
                println!("counting {:?}", i);
                thread::sleep(Duration::from_millis(500));
            }
            Ok(10) as Result<_, ()>
        });

        let b = executor.spawn_fn(|| {
            for i in 20..30 {
                println!("counting {:?}", i);
                thread::sleep(Duration::from_millis(700));
            }
            Ok(20) as Result<_, ()>
        });

        let timer = Timer::default();
        let timeout = timer.sleep(Duration::from_millis(1000)).then(|_| Err(()));

        let c = a.join(b).map(|(a, b)| a + b);

        let d = timeout.select(c).map(|(win, _)| win);

        match d.wait() {
            Ok(val) => println!("Ok({:?})", val),
            Err(_) => println!("Timed out"),
        }
    }
}
