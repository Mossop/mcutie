use core::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use embassy_sync::blocking_mutex::{raw::RawMutex, Mutex};
use pin_project::pin_project;

struct PipeData<T, const N: usize> {
    connect_count: usize,
    receiver_waker: Option<Waker>,
    sender_waker: Option<Waker>,
    pending: Option<T>,
}

fn swap_wakers(waker: &mut Option<Waker>, new_waker: &Waker) {
    if let Some(old_waker) = waker.take() {
        if old_waker.will_wake(new_waker) {
            *waker = Some(old_waker)
        } else {
            if !new_waker.will_wake(&old_waker) {
                old_waker.wake();
            }

            *waker = Some(new_waker.clone());
        }
    } else {
        *waker = Some(new_waker.clone())
    }
}

pub(crate) struct ReceiveFuture<'a, M: RawMutex, T, const N: usize> {
    pipe: &'a ConnectedPipe<M, T, N>,
}

impl<M: RawMutex, T, const N: usize> Future for ReceiveFuture<'_, M, T, N> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.pipe.inner.lock(|cell| {
            let mut inner = cell.borrow_mut();

            if let Some(waker) = inner.sender_waker.take() {
                waker.wake();
            }

            if let Some(item) = inner.pending.take() {
                if let Some(old_waker) = inner.receiver_waker.take() {
                    old_waker.wake();
                }

                Poll::Ready(item)
            } else {
                swap_wakers(&mut inner.receiver_waker, cx.waker());
                Poll::Pending
            }
        })
    }
}

pub(crate) struct PipeReader<'a, M: RawMutex, T, const N: usize> {
    pipe: &'a ConnectedPipe<M, T, N>,
}

impl<M: RawMutex, T, const N: usize> PipeReader<'_, M, T, N> {
    #[must_use]
    pub(crate) fn receive(&self) -> ReceiveFuture<'_, M, T, N> {
        ReceiveFuture { pipe: self.pipe }
    }
}

impl<M: RawMutex, T, const N: usize> Drop for PipeReader<'_, M, T, N> {
    fn drop(&mut self) {
        self.pipe.inner.lock(|cell| {
            let mut inner = cell.borrow_mut();
            inner.connect_count -= 1;

            if inner.connect_count == 0 {
                inner.pending = None;
            }

            if let Some(waker) = inner.sender_waker.take() {
                waker.wake();
            }
        })
    }
}

#[pin_project]
pub(crate) struct PushFuture<'a, M: RawMutex, T, const N: usize> {
    data: Option<T>,
    pipe: &'a ConnectedPipe<M, T, N>,
}

impl<M: RawMutex, T, const N: usize> Future for PushFuture<'_, M, T, N> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.pipe.inner.lock(|cell| {
            let project = self.project();
            let mut inner = cell.borrow_mut();

            if let Some(receiver) = inner.receiver_waker.take() {
                receiver.wake();
            }

            if project.data.is_none() || inner.connect_count == 0 {
                trace!("Dropping packet");
                Poll::Ready(())
            } else if inner.pending.is_some() {
                swap_wakers(&mut inner.sender_waker, cx.waker());
                Poll::Pending
            } else {
                inner.pending = project.data.take();

                Poll::Ready(())
            }
        })
    }
}

/// A pipe that knows whether a receiver is connected. If so pushing to the
/// queue waits until there is space in the queue, otherwise data is simply
/// dropped.
pub(crate) struct ConnectedPipe<M: RawMutex, T, const N: usize> {
    inner: Mutex<M, RefCell<PipeData<T, N>>>,
}

impl<M: RawMutex, T, const N: usize> ConnectedPipe<M, T, N> {
    pub(crate) const fn new() -> Self {
        Self {
            inner: Mutex::new(RefCell::new(PipeData {
                connect_count: 0,
                receiver_waker: None,
                sender_waker: None,
                pending: None,
            })),
        }
    }

    /// A future that waits for a new item to be available.
    pub(crate) fn reader(&self) -> PipeReader<'_, M, T, N> {
        self.inner.lock(|cell| {
            let mut inner = cell.borrow_mut();
            inner.connect_count += 1;

            PipeReader { pipe: self }
        })
    }

    /// Pushes an item to the reader, waiting for a slot to become available if
    /// connected.
    #[must_use]
    pub(crate) fn push(&self, data: T) -> PushFuture<'_, M, T, N> {
        PushFuture {
            data: Some(data),
            pipe: self,
        }
    }
}


#[cfg(test)]
mod tests {
    use core::time::Duration;

    use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
    use futures_executor::{LocalPool, ThreadPool};
    use futures_util::{future::select, pin_mut, task::SpawnExt, FutureExt};
    use futures_timer::Delay;

    use super::ConnectedPipe;

    async fn wait_milis(milis: u64) {
        Delay::new(Duration::from_millis(milis)).await;
    }

    // #[futures_test::test]
    #[test]
    fn test_send_receive() {
        let mut executor = LocalPool::new();
        let spawner = executor.spawner();

        static PIPE: ConnectedPipe<CriticalSectionRawMutex, usize, 5> = ConnectedPipe::new();

        // Task that sends 
        spawner.spawn(async {
            wait_milis(10).await;

            PIPE.push(23).await;
            PIPE.push(56).await;
            PIPE.push(67).await;
        }).unwrap();


        // Task that receives
        spawner.spawn(async {
            let reader = PIPE.reader();
            let value = reader.receive().await;
            assert_eq!(value, 23);
            let value = reader.receive().await;
            assert_eq!(value, 56);
            let value = reader.receive().await;
            assert_eq!(value, 67);

        }).unwrap();

        executor.run();
    }

    #[futures_test::test]
    async fn test_send_drop() {

        static PIPE: ConnectedPipe<CriticalSectionRawMutex, usize, 5> = ConnectedPipe::new();

        PIPE.push(23).await;
        PIPE.push(56).await;
        PIPE.push(67).await;


        // Create a reader after sending
        let reader = PIPE.reader();
        let receive = reader.receive()
            .fuse();
        pin_mut!(receive);

        let timeout = wait_milis(50).fuse();
        pin_mut!(timeout);

        let either = select(receive, timeout).await;
        
        match either {
            futures_util::future::Either::Left(_) => {
                panic!("There should be nothing to receive!");
            },
            futures_util::future::Either::Right(_) => {},
        }
    }

    #[futures_test::test]
    async fn test_bulk_send_publish() {
        static PIPE: ConnectedPipe<CriticalSectionRawMutex, usize, 5> = ConnectedPipe::new();

        let executor = ThreadPool::new().unwrap();

        executor.spawn(async {
            for i in 0..1000 {
                PIPE.push(i).await;
            }
        }).unwrap();

        executor.spawn(async {
            for i in 1000..2000 {
                PIPE.push(i).await;
            }
        }).unwrap();

        let reader = PIPE.reader();
        for _ in 0..800 {
            reader.receive().await;
        }

    }

}
