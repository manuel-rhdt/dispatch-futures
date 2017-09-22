extern crate dispatch;
extern crate futures;
extern crate stash;
extern crate tokio_timer;

use futures::{Async, Future};
use futures::future::{Executor, ExecuteError, IntoFuture};
use futures::executor;
use futures::executor::{Spawn, Notify};
use futures::sync::oneshot::{spawn, spawn_fn, SpawnHandle, Execute};
use stash::Stash;

use std::mem;
use std::sync::{Arc, Weak, Mutex};
use std::ptr;

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

struct Task {
    future: Spawn<BoxFuture>,
}

pub struct Notifier {
    queue: DispatchQueue,
}

impl executor::Notify for Notifier {
    fn notify(&self, id: usize) {
        self.queue.send_work(id);
    }
}

#[derive(Clone)]
pub struct Queue {
    inner: Arc<Inner>,
}

pub struct Inner {
    pub queue: dispatch::Queue,
}

impl Queue {
    fn submit(&self, task: SpawnedTask) {
        // submit work
        let inner = self.inner.clone();
        self.inner.queue.async(move || {
            // Get the notifier.
            let notify = Arc::new(Notfer {
                inner: Arc::downgrade(&inner),
            });
            task.run(&notify)
        });
    }
}

pub struct TaskInner {
    future: Option<Spawn<BoxFuture>>,
}

pub struct SpawnedTask {
    ptr: *mut TaskInner,
}

impl SpawnedTask {
    pub fn new<T: Future<Item = (), Error = ()> + Send + 'static>(f: T) -> SpawnedTask {
        let inner = Box::new(TaskInner {
            future: Some(executor::spawn(Box::new(f))),
        });

        SpawnedTask { ptr: Box::into_raw(inner) }
    }

    /// Transmute a u64 to a Task
    pub unsafe fn from_notify_id(unpark_id: usize) -> SpawnedTask {
        mem::transmute(unpark_id)
    }

    /// Transmute a u64 to a task ref
    pub unsafe fn from_notify_id_ref<'a>(unpark_id: &'a usize) -> &'a SpawnedTask {
        mem::transmute(unpark_id)
    }

    pub fn run(&self, notify: &Arc<Notfer>) {
        let _ = self.inner_mut().future.as_mut().unwrap()
            .poll_future_notify(notify, self.ptr as usize);
    }

    #[inline]
    fn inner(&self) -> &TaskInner {
        unsafe { &*self.ptr }
    }

    #[inline]
    fn inner_mut(&self) -> &mut TaskInner {
        unsafe { &mut *self.ptr }
    }
}

impl Clone for SpawnedTask {
    fn clone(&self) -> Self {
        SpawnedTask {
            ptr: self.ptr
        }
    }
}

unsafe impl Send for SpawnedTask {}

pub struct Notfer {
    inner: Weak<Inner>,
}

impl executor::Notify for Notfer {
    fn notify(&self, id: usize) {
        let task = unsafe { SpawnedTask::from_notify_id_ref(&id) };
        let queue = Queue {
            inner: self.inner.upgrade().unwrap()
        };
        queue.submit(task.clone());
    }
}

impl<F> Executor<F> for Queue
where
    F: Future<Item = (), Error = ()> + Send + Sync + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        let task = SpawnedTask::new(future);
        self.submit(task);
        Ok(())
    }
}

#[derive(Clone)]
pub struct DispatchQueue {
    queue: dispatch::Queue,
    tasks: Arc<Mutex<Stash<Option<Task>>>>,
    notify: Arc<Notifier>,
}

unsafe impl Sync for DispatchQueue {}

impl DispatchQueue {
    pub fn with_queue(q: dispatch::Queue) -> Self {
        let mut queue = DispatchQueue {
            queue: q,
            tasks: Arc::new(Mutex::new(Stash::new())),
            // leave notify temporarily uninitialized
            notify: unsafe { Arc::new(::std::mem::zeroed()) },
        };

        let notifier = Notifier { queue: queue.clone() };

        // Write into uninitialized memory without dropping.
        let raw_ptr = Arc::into_raw(queue.notify) as *mut _;
        unsafe { ptr::write(raw_ptr, notifier) };
        unsafe { queue.notify = Arc::from_raw(raw_ptr) };

        queue
    }

    fn send_work(&self, index: usize) {
        let slot = self.tasks
            .lock()
            .expect("Could not lock mutex")
            .get_mut(index)
            .map(|slot| slot.take());

        let mut task = match slot {
            Some(Some(task)) => task,
            _ => {
                // Slot empty, task already in progress.
                return;
            }
        };
        let tasks = self.tasks.clone();
        let notify = self.notify.clone();

        self.queue.async(move || {
            let res = task.future.poll_future_notify(&notify, index);
            match res {
                Ok(Async::NotReady) => {
                    tasks.lock().expect("Could not lock mutex")[index] = Some(task);
                }
                Ok(Async::Ready(())) | Err(()) => {
                    tasks.lock().expect("Could not lock mutex").take(index).unwrap();
                }
            }
        });
    }

    pub fn spawn<F>(&self, future: F) -> DispatchFuture<F::Item, F::Error>
    where
        F: Future,
        Self: Executor<Execute<F>>,
    {
        DispatchFuture { inner: spawn(future, self) }
    }

    pub fn spawn_fn<F, R>(&self, f: F) -> DispatchFuture<R::Item, R::Error>
    where
        F: FnOnce() -> R + Send,
        R: IntoFuture,
        Self: Executor<Execute<futures::Lazy<F, R>>>,
    {
        DispatchFuture { inner: spawn_fn(f, self) }
    }
}

impl<F> Executor<F> for DispatchQueue
where
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        let spawned_future = executor::spawn(Box::new(future) as BoxFuture);

        let index = {
            let mut tasks = self.tasks.lock().expect("Could not lock mutex");

            let index = tasks.put(None);

            tasks[index] = Some(Task { future: spawned_future });

            index
        };

        self.send_work(index);
        Ok(())
    }
}

impl Default for DispatchQueue {
    fn default() -> DispatchQueue {
        DispatchQueue::with_queue(dispatch::Queue::global(dispatch::QueuePriority::Default))
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
