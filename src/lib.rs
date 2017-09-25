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
//!

extern crate dispatch;
extern crate futures;
#[macro_use]
extern crate log;

mod task;
mod notify_mutex;

use futures::{Async, Future};
use futures::future::{Executor, ExecuteError, IntoFuture};
use futures::executor;
use futures::sync::oneshot::{spawn, spawn_fn, SpawnHandle, Execute};

use std::{ptr, mem, fmt};
use std::cell::UnsafeCell;
use std::sync::{Arc, Weak};
use std::ops::Deref;

use task::Task;

type BoxFuture = Box<Future<Item = (), Error = ()> + Send + 'static>;

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
/// You can `.wait()` on a `EnqueuedFuture` to block until the computation finishes:
/// TODO
/// ```
/// # extern crate dispatch_futures;
/// # extern crate futures;
///
/// use dispatch_futures::QueueExecutor;
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
/// future.wait().unwrap();
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
    /// useful for logging and other unimportant tasks that need to be scheduled ansynchronously.
    pub fn forget(self) {
        self.inner.forget()
    }

    /// Returns the queue onto which the contained future was enqueued.
    pub fn queue(&self) -> QueueExecutor {
        QueueExecutor { inner: self.queue_inner.clone() }
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
/// `QueueExecutor`'s deref implementation.
///
/// Examples
/// --------
///
/// Create an executor from the global parallel queue with default priority
///
/// ```
/// use dispatch_futures::QueueExecutor;
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
    // UnsafeCell is needed here so we can construct a reference cycle (Notifier contains
    // a Weak<Inner>). Unsafe access is only needed during construction of a `QueueExecutor`.
    notifier: UnsafeCell<Arc<Notifier>>,
}

impl Inner {
    fn notifier_ref(&self) -> &Arc<Notifier> {
        unsafe { &*self.notifier.get() }
    }
}

// The thread-unsafe mutation of the contained `UnsafeCell` is only performed during initialization
// of a new `QueueExecutor`. Thus the following impl is safe.
unsafe impl Sync for Inner {}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Queue")
            .field("label", &self.queue.label())
            .finish()
    }
}

#[derive(Debug)]
struct Notifier {
    inner: Weak<Inner>,
}

impl executor::Notify for Notifier {
    fn notify(&self, id: usize) {
        trace!("notifiy id: {:?}", id);

        // Construct the dispatch queue from the pointer to `inner`...
        let queue = QueueExecutor {
            inner: self.inner.upgrade().unwrap_or_else(|| {
                panic!(
                    "Task with id={:?} was notified while QueueExecutor was already freed!",
                    id
                )
            }),
        };

        // ... and submit the task to be polled again.
        let task = unsafe { Task::from_notify_id_ref(&id) };
        queue.submit(task.clone());
    }

    fn clone_id(&self, id: usize) -> usize {
        trace!("clone_id: {:?}", id);
        unsafe {
            let handle = Task::from_notify_id_ref(&id);
            mem::forget(handle.clone());
        }

        id
    }

    fn drop_id(&self, id: usize) {
        trace!("drop_id: {:?}", id);
        unsafe {
            let _ = Task::from_notify_id(id);
        }
    }
}

impl<F> Executor<F> for QueueExecutor
where
    F: Future<Item = (), Error = ()> + Send + Sync + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        trace!("QueueExecutor::execute");
        let task = Task::new(future);
        self.submit(task);
        Ok(())
    }
}

impl QueueExecutor {
    /// Create a new `QueueExecutor` from an existing `queue` wrapped using the `dispatch` crate.
    pub fn new(queue: dispatch::Queue) -> QueueExecutor {
        queue.into()
    }

    fn submit(&self, task: Task) {
        trace!("QueueExecutor::submit({:?})", task);
        // test if we should poll the future
        let mut future = match task.mutex.notify() {
            // Ok means we may queue up the future and poll it.
            Ok(future) => future,
            // Err means the future is already being polled or has already completed.
            // In this case there is nothing to do and we just return.
            Err(_) => {
                trace!("Already scheduled to poll or already completed.");
                return;
            }
        };
        let inner = self.inner.clone();
        self.inner.queue.async(move || {
            let notifier = inner.notifier_ref();
            unsafe {
                // notify the mutex that we are about to poll
                task.mutex.start_poll();
            }

            // loop until no more repolling is needed
            loop {
                trace!("Poll: started");
                match future.poll_future_notify(notifier, task.notify_id()) {
                    Ok(Async::Ready(())) |
                    Err(()) => {
                        trace!("Poll: completed");
                        unsafe { task.mutex.complete() };
                        break;
                    }
                    Ok(Async::NotReady) => {
                        trace!("Poll: not ready");
                        match unsafe { task.mutex.wait(future) } {
                            // no repolling needed
                            Ok(()) => break,
                            // Poll the future again because it was notified during the poll.
                            Err(repoll_future) => future = repoll_future,
                        }
                    }
                }
            }
        });
    }

    /// Immediately submits the provided future to the queue which will then be driven to
    /// completion.
    ///
    /// Returns a Future representing the finished result after the provided future has completed.
    pub fn spawn<F>(&self, future: F) -> EnqueuedFuture<F::Item, F::Error>
    where
        F: Future,
        Self: Executor<Execute<F>>,
    {
        EnqueuedFuture {
            inner: spawn(future, self),
            queue_inner: self.inner.clone(),
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
    where
        F: FnOnce() -> R + Send,
        R: IntoFuture,
        Self: Executor<Execute<futures::Lazy<F, R>>>,
    {
        EnqueuedFuture {
            inner: spawn_fn(f, self),
            queue_inner: self.inner.clone(),
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
            queue: queue,
            notifier: UnsafeCell::new(Arc::new(Notifier { inner: Weak::new() })),
        });
        let notifier = Arc::new(Notifier { inner: Arc::downgrade(&inner) });
        unsafe { ptr::replace(inner.notifier.get(), notifier) };
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
