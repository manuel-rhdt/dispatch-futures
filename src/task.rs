use futures::prelude::*;
use futures::executor;

use std::{mem, ptr, fmt};
use std::sync::Arc;

use super::BoxFuture;
use super::notify_mutex::NotifyMutex;

#[derive(Clone)]
pub struct Task {
    pub mutex: Arc<NotifyMutex<executor::Spawn<BoxFuture>>>,
}

impl Task {
    pub fn new<T: Future<Item = (), Error = ()> + Send + 'static>(f: T) -> Task {
        let future = executor::spawn(Box::new(f) as BoxFuture);
        let mutex = NotifyMutex::new(future);
        Task { mutex: Arc::new(mutex) }
    }

    pub fn notify_id(&self) -> usize {
        unsafe {
            let this: Task = ptr::read(self);
            mem::transmute(this)
        }
    }

    /// Transmute a u64 to a Task reference
    pub unsafe fn from_notify_id_ref(notify_id: &usize) -> &Task {
        mem::transmute(notify_id)
    }

    /// Transmute a u64 to a Task
    pub unsafe fn from_notify_id(notify_id: usize) -> Task {
        mem::transmute(notify_id)
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Task")
            .field("id", &self.notify_id())
            .finish()
    }
}
