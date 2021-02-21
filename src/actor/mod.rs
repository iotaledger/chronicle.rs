use async_trait::async_trait;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub use builder::*;
pub use end::End;
pub use event_loop::EventLoop;
pub use init::Init;
pub use launcher::*;
pub use passthrough::Passthrough;
pub use start::StartActor;
pub use starter::Starter;
pub use supervisor::*;
pub use terminating::Terminating;

#[async_trait]
pub trait Actor<H: AknShutdown<Self> + 'static>: StartActor<H> {}

/// Runtime to be implemented on the Service
pub trait Runtime {
    fn spawn<T>(task: T) -> tokio::task::JoinHandle<T::Output>
    where
        T: core::future::Future + Send + 'static,
        T::Output: Send + 'static,
    {
        tokio::spawn(task)
    }
    fn spawn_child<T>(&mut self, task: T, service: Self) -> tokio::task::JoinHandle<T::Output>
    where
        T: core::future::Future + Send + 'static,
        T::Output: Send + 'static;
    fn sleep(duration: core::time::Duration) -> tokio::time::Sleep {
        tokio::time::sleep(duration)
    }
    fn sleep_until(deadline: tokio::time::Instant) -> tokio::time::Sleep {
        tokio::time::sleep_until(deadline)
    }
    fn yield_now() -> YieldNow {
        YieldNow::default()
    }
}

impl Runtime for Service {
    fn spawn_child<T>(&mut self, task: T, service: Self) -> tokio::task::JoinHandle<T::Output>
    where
        T: core::future::Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.update_microservice(service.get_name(), service);
        Self::spawn(task)
    }
}

/// Global Tokio YieldNow struct
#[derive(Default)]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }

        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl<T: super::EventLoop<H> + super::Init<H> + super::Terminating<H>, H: Send + 'static + AknShutdown<Self>> Actor<H> for T {}

impl<T, H: 'static> StartActor<H> for T
where
    T: Actor<H>,
    H: AknShutdown<T>,
{
}

impl<T, H: 'static> End<H> for T
where
    T: Actor<H>,
    H: AknShutdown<T>,
{
}

mod builder;
mod end;
mod event_loop;
mod init;
mod launcher;
mod passthrough;
mod start;
mod starter;
mod supervisor;
mod terminating;
