use async_trait::async_trait;
use std::time::Duration;

pub enum Need {
    Restart,
    RescheduleAfter(Duration),
    Abort,
}

/// Should be implemented on the Actor(or pid) that should become a child
pub trait Child<H, S>: Send {
    fn handle_shutdown(self, handle: Option<H>, supervisor_state: &mut S)
    where
        Self: Sized;
}

/// Should be implemented on the supervisor_handle
#[async_trait]
pub trait AknShutdown<A>: Send {
    async fn aknowledge_shutdown(self, state: A, status: Result<(), Need>);
}

/// Should be implemented on the child_shutdown_handle
pub trait Shutdown: Send {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized;
}

/// NoneSupervisor
pub struct NullSupervisor;
impl NullSupervisor {
    pub fn new() -> Option<Self> {
        Some(Self)
    }
}
#[async_trait]
impl<A: Send + 'static> AknShutdown<A> for NullSupervisor {
    async fn aknowledge_shutdown(self, _state: A, _status: Result<(), Need>) {}
}
