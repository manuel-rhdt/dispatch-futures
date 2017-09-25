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

use std::{ptr, mem};
use std::cell::UnsafeCell;
use std::sync::{Arc, Weak};

use task::Task;

type BoxFuture = Box<Future<Item = (), Error = ()> + Send + 'static>;

/// A future acting as a proxy to the original future passed to `Queue::spawn`.
///
/// A `DispachFuture` represents a calculation performed on a dispatch queue.
/// If the `DispatchFuture` is dropped before it completed its execution on the gcd queue will tried
/// to be canceled. If you want to have the future execute in the background and discarding its
/// result see the `forget()` method.
///
/// Examples
/// --------
///
/// You can `.wait()` on a `DispatchFuture` to block until the computation finishes:
///
/// ```
/// # extern crate dispatch_futures;
/// # extern crate futures;
///
/// use dispatch_futures::DispatchQueue;
/// use futures::future::Future;
///
/// # fn main() {
/// let executor = DispatchQueue::default();
/// let future = executor.spawn_fn(|| {
/// #   let result = 0;
///     // long computation...
///     Ok(result) as Result<_, ()>
/// });
///
/// // This will not return until the execution of the closure has completed
/// // (probably on another thread)
/// future.wait().unwrap();
/// # }
/// ```
///
pub struct DispatchFuture<T, E> {
    inner: SpawnHandle<T, E>,
}

impl<T, E> DispatchFuture<T, E> {
    pub fn forget(self) {
        self.inner.forget()
    }
}

impl<T: Send, E> Future for DispatchFuture<T, E> {
    type Item = T;
    type Error = E;
    fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

/// A Wrapper of a dispatch queue that can execute futures.
#[derive(Clone)]
pub struct DispatchQueue {
    inner: Arc<Inner>,
}

struct Inner {
    queue: dispatch::Queue,
    // UnsafeCell is needed here so we can construct a reference cycle (Notifier contains
    // a Weak<Inner>).
    notifier: UnsafeCell<Arc<Notifier>>,
}

impl Inner {
    pub fn notifier_ref(&self) -> &Arc<Notifier> {
        unsafe { &*self.notifier.get() }
    }
}

unsafe impl Sync for Inner {}

impl DispatchQueue {
    fn submit(&self, task: Task) {
        trace!("Queue::submit({:?})", task);
        // test if we should poll the future
        let mut future = match task.mutex.notify() {
            // we may queue up the future to be polled
            Ok(future) => future,
            // The future is already being polled or has already completed.
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
                            // poll the future again
                            Err(repoll_future) => future = repoll_future,
                        }
                    }
                }
            }
        });
    }
}

struct Notifier {
    inner: Weak<Inner>,
}

impl executor::Notify for Notifier {
    fn notify(&self, id: usize) {
        trace!("notifiy id: {:?}", id);
        let queue = DispatchQueue {
            inner: self.inner.upgrade().unwrap_or_else(|| {
                panic!(
                    "Task with id={:?} was notified while Queue was already freed!",
                    id
                )
            }),
        };
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

impl<F> Executor<F> for DispatchQueue
where
    F: Future<Item = (), Error = ()> + Send + Sync + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        trace!("Queue::execute");
        let task = Task::new(future);
        self.submit(task);
        Ok(())
    }
}

impl DispatchQueue {
    /// Create a new `DispatchQueue` from `queue`.
    pub fn new(queue: dispatch::Queue) -> DispatchQueue {
        let inner = Arc::new(Inner {
            queue: queue,
            notifier: UnsafeCell::new(Arc::new(Notifier { inner: Weak::new() })),
        });
        let notifier = Arc::new(Notifier { inner: Arc::downgrade(&inner) });
        unsafe { ptr::replace(inner.notifier.get(), notifier) };
        DispatchQueue { inner: inner }
    }

    /// Immediately submits the provided future to the queue. Returns a Future representing
    /// the finished result after the provided future has completed.
    pub fn spawn<F>(&self, future: F) -> DispatchFuture<F::Item, F::Error>
    where
        F: Future,
        Self: Executor<Execute<F>>,
    {
        DispatchFuture { inner: spawn(future, self) }
    }

    /// Immediately submits the provided closure to the queue. Returns a Future representing
    /// the finished result after executing the closure.
    ///
    /// This is identical to `executor.spawn(lazy(f))`.
    pub fn spawn_fn<F, R>(&self, f: F) -> DispatchFuture<R::Item, R::Error>
    where
        F: FnOnce() -> R + Send,
        R: IntoFuture,
        Self: Executor<Execute<futures::Lazy<F, R>>>,
    {
        DispatchFuture { inner: spawn_fn(f, self) }
    }
}

impl Default for DispatchQueue {
    fn default() -> Self {
        DispatchQueue::new(dispatch::Queue::global(dispatch::QueuePriority::Default))
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
        let executor = DispatchQueue::default();
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
