use async_trait::async_trait;
use std::time::Duration;

pub enum Need {
    Restart,
    RescheduleAfter(Duration),
    Abort(AbortType),
}

#[repr(u8)]
pub enum AbortType {
    Error = 0,
    Manual = 1,
    Timeout = 2,
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
