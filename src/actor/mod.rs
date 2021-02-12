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
pub use name::Name;
pub use passthrough::Passthrough;
pub use preparer::Preparer;
pub use start::StartActor;
pub use starter::Starter;
pub use supervisor::*;
pub use terminating::Terminating;

#[async_trait]
pub trait Actor<H: AknShutdown<Self> + 'static>: StartActor<H> + Name {
    fn spawn<T>(task: T) -> tokio::task::JoinHandle<T::Output>
    where
        T: core::future::Future + Send + 'static,
        T::Output: Send + 'static,
    {
        tokio::spawn(task)
    }
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

impl<T: super::Name + super::EventLoop<H> + super::Init<H> + super::Terminating<H>, H: Send + 'static + AknShutdown<Self>> Actor<H> for T {}

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
mod name;
mod passthrough;
mod preparer;
mod start;
mod starter;
mod supervisor;
mod terminating;
