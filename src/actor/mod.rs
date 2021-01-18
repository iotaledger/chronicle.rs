use async_trait::async_trait;

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
