extern crate dispatch;
extern crate futures;
extern crate tokio_timer;
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

#[derive(Clone)]
pub struct Queue {
    inner: Arc<Inner>,
}

pub struct Inner {
    pub queue: dispatch::Queue,
    notifier: UnsafeCell<Arc<Notifier>>,
}

impl Inner {
    pub fn notifier_ref(&self) -> &Arc<Notifier> {
        unsafe { &*self.notifier.get() }
    }
}

unsafe impl Sync for Inner {}

impl Queue {
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
                return
            },
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

pub struct Notifier {
    inner: Weak<Inner>,
}

impl executor::Notify for Notifier {
    fn notify(&self, id: usize) {
        trace!("notifiy id: {:?}", id);
        let queue = Queue {
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

impl<F> Executor<F> for Queue
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

impl Queue {
    pub fn new() -> Queue {
        let inner = Arc::new(Inner {
            queue: dispatch::Queue::global(dispatch::QueuePriority::Default),
            notifier: UnsafeCell::new(Arc::new(Notifier { inner: Weak::new() })),
        });
        let notifier = Arc::new(Notifier { inner: Arc::downgrade(&inner) });
        unsafe { ptr::replace(inner.notifier.get(), notifier) };
        Queue { inner: inner }
    }

    pub fn spawn<F>(&self, future: F) -> DispatchFuture<F::Item, F::Error>
    where
        F: Future,
        Self: Executor<Execute<F>>,
    {
        DispatchFuture { inner: spawn(future, self) }
    }

    /// Immediately submits the provided closure to the queue. Returns a Future representing
    /// the finished result after executing the closure. 
    pub fn spawn_fn<F, R>(&self, f: F) -> DispatchFuture<R::Item, R::Error>
    where
        F: FnOnce() -> R + Send,
        R: IntoFuture,
        Self: Executor<Execute<futures::Lazy<F, R>>>,
    {
        DispatchFuture { inner: spawn_fn(f, self) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio_timer::*;

    use std::thread;
    use std::time::Duration;

    #[test]
    fn it_works() {
        let executor = Queue::new();
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
