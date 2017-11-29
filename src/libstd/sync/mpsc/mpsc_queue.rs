// Copyright 2017 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A mostly lock-free multi-producer, single consumer queue.
//!
//! This module contains an implementation of a concurrent MPSC queue. This
//! queue can be used to share data between threads, and is also used as the
//! building block of channels in rust.
//!
//! Note that the current implementation of this queue has a caveat of the `pop`
//! method, and see the method for more information about it. Due to this
//! caveat, this queue may not be appropriate for all use-cases.

// http://www.1024cores.net/home/lock-free-algorithms
//                         /queues/non-intrusive-mpsc-node-based-queue

pub use self::PopResult::*;

use alloc::boxed::Box;
use core::ptr;
use core::cell::UnsafeCell;

use sync::atomic::{AtomicPtr, Ordering};

/// A result of the `pop` function.
pub enum PopResult<T> {
    /// Some data has been popped
    Data(T),
    /// The queue is empty
    Empty,
    /// The queue is in an inconsistent state. Popping data should succeed, but
    /// some pushers have yet to make enough progress in order allow a pop to
    /// succeed. It is recommended that a pop() occur "in the near future" in
    /// order to see if the sender has made progress or not.
    ///
    /// HD: the word "inconsistent" comes with the phrase "window of inconsistency",
    /// which is the case in which, for the consumer, head and tail of the queue are
    /// still the same (ie. the queue is empty), but the next field of tail is not null.
    /// This is the exact moment when the producer has already put the new element
    /// in the linked list, but has not set head to point to this new element (or it
    /// already has, but the change has not reach the consumer yet). This is in between
    /// the queue's transition from the empty state to a non-empty state (the window).
    /// The queue technically is non-empty, but the push has not finished yet, so it is
    /// logically empty.
    Inconsistent,
}

/// HD: Node structure of the MPSC queue
/// This queue supports pushing to the head, and poping from the tail of the queue
/// The next field is from the perspective of the consumer: the consumer will pop
/// the next node from the tail. The links are in the direction from tail to head.
/// The tail node is always a sentinel node.
struct Node<T> {
    next: AtomicPtr<Node<T>>,
    value: Option<T>,
}

/// The multi-producer single-consumer structure. This is not cloneable, but it
/// may be safely shared so long as it is guaranteed that there is only one
/// popper at a time (many pushers are allowed).
///
/// HD: The head is shared atomically among producers and the consumer, while the tail is
/// is owned uniquely and non-atomically by the consumer.
pub struct Queue<T> {
    head: AtomicPtr<Node<T>>,
    tail: UnsafeCell<*mut Node<T>>,
}

unsafe impl<T: Send> Send for Queue<T> { }
/// HD: the queue is not sync for multiple consumers.
/// TODO: This unsafety is currently not reflected.
unsafe impl<T: Send> Sync for Queue<T> { }

impl<T> Node<T> {
    unsafe fn new(v: Option<T>) -> *mut Node<T> {
        Box::into_raw(box Node {
            next: AtomicPtr::new(ptr::null_mut()),
            value: v,
        })
    }
}

impl<T> Queue<T> {
    /// Creates a new queue that is safe to share among multiple producers and
    /// one consumer.
    ///
    /// HD: Initialization creates a sentinel node. There is always one sentinel node, which
    /// the tail always points to.
    pub fn new() -> Queue<T> {
        let stub = unsafe { Node::new(None) };
        Queue {
            head: AtomicPtr::new(stub),
            tail: UnsafeCell::new(stub),
        }
    }

    /// Pushes a new value onto this queue.
    ///
    /// HD: Pushes replace head with the new node, before setting the old head's link
    /// to this new node. In between these two actions is the window of inconsistency.
    /// The swap is AcqRel, to sync with other swaps from other producers.
    /// The link's store is Rel, to sync with the consumer's Acq load. This Rel store releases
    /// the permission to acquire the node.
    pub fn push(&self, t: T) {
        unsafe {
            let n = Node::new(Some(t));
            let prev = self.head.swap(n, Ordering::AcqRel);
            (*prev).next.store(n, Ordering::Release);
        }
    }

    /// Pops some data from this queue.
    ///
    /// Note that the current implementation means that this function cannot
    /// return `Option<T>`. It is possible for this queue to be in an
    /// inconsistent state where many pushes have succeeded and completely
    /// finished, but pops cannot return `Some(t)`. This inconsistent state
    /// happens when a pusher is pre-empted at an inopportune moment.
    ///
    /// This inconsistent state means that this queue does indeed have data, but
    /// it does not currently have access to it at this time.
    ///
    /// HD: the Acq load of the next element from tail is sync-ing with the Rel store of push.
    /// If the load of this next field is non-null, the load acquires the permission to use
    /// the node.
    /// If the tail's link (the next) is null, the queue is logically empty (subjectively for
    /// the consumer). In this case, if head == tail then, subjective for the consumer, the queue
    /// is really empty. Otherwise it is in the window of inconsistency.
    /// The Acq load of head is unnecessary, and can be replaced with Relaxed. TODO: proof.
    pub fn pop(&self) -> PopResult<T> {
        unsafe {
            let tail = *self.tail.get();
            let next = (*tail).next.load(Ordering::Acquire);

            if !next.is_null() {
                *self.tail.get() = next;
                assert!((*tail).value.is_none());
                assert!((*next).value.is_some());
                let ret = (*next).value.take().unwrap();
                let _: Box<Node<T>> = Box::from_raw(tail);
                return Data(ret);
            }

            if self.head.load(Ordering::Acquire) == tail {Empty} else {Inconsistent}
        }
    }
}

/// HD: dropping starts from tail, utilizing Box's drop. This means that only the tail's owner
/// should call drop: the tail transitively owns the queue. However, the code does not make it
/// clear when one should call drop, and the Relaxed load does not guarantess the absence of
/// memory leaks. In fact, while it is completely safe to drop while there are concurrent pushes,
/// the updates from the pushes do not necessarily reach the dropping thread in time, making
/// cur.is_null return true and the loop break, thus leaking memory. Making the load Acq does not
/// help either. To avoid memory leaks, the last pushes of the producers must be sync-ed to the
/// thread before the thread calls drop.
impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let mut cur = *self.tail.get();
            while !cur.is_null() {
                let next = (*cur).next.load(Ordering::Relaxed);
                let _: Box<Node<T>> = Box::from_raw(cur);
                cur = next;
            }
        }
    }
}

#[cfg(all(test, not(target_os = "emscripten")))]
mod tests {
    use sync::mpsc::channel;
    use super::{Queue, Data, Empty, Inconsistent};
    use sync::Arc;
    use thread;

    #[test]
    fn test_full() {
        let q: Queue<Box<_>> = Queue::new();
        q.push(box 1);
        q.push(box 2);
    }

    #[test]
    fn test() {
        let nthreads = 8;
        let nmsgs = 1000;
        let q = Queue::new();
        match q.pop() {
            Empty => {}
            Inconsistent | Data(..) => panic!()
        }
        let (tx, rx) = channel();
        let q = Arc::new(q);

        for _ in 0..nthreads {
            let tx = tx.clone();
            let q = q.clone();
            thread::spawn(move|| {
                for i in 0..nmsgs {
                    q.push(i);
                }
                tx.send(()).unwrap();
            });
        }

        let mut i = 0;
        while i < nthreads * nmsgs {
            match q.pop() {
                Empty | Inconsistent => {},
                Data(_) => { i += 1 }
            }
        }
        drop(tx);
        for _ in 0..nthreads {
            rx.recv().unwrap();
        }
    }
}
