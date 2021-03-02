use async_trait::async_trait;
use std::time::Duration;

#[derive(Clone, Copy)]
/// Need enum used by children to ask their supervisors
pub enum Need {
    /// Child is asking for Restart the actor/service
    Restart,
    /// Child is asking for RescheduleAfter
    RescheduleAfter(Duration),
    /// Child is asking for Abort
    Abort,
}

/// Should be implemented on the supervisor_handle
#[async_trait]
pub trait AknShutdown<A>: Send {
    /// Child aknowledging shutdown for its supervisor
    async fn aknowledge_shutdown(self, state: A, status: Result<(), Need>);
}

/// Should be implemented on the child_shutdown_handle
pub trait Shutdown: Send {
    /// Shutdown the service
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized;
}

/// NoneSupervisor
pub struct NullSupervisor;
impl NullSupervisor {
    /// Create NullSupervisor
    pub fn new() -> Option<Self> {
        Some(Self)
    }
}
#[async_trait]
impl<A: Send + 'static> AknShutdown<A> for NullSupervisor {
    async fn aknowledge_shutdown(self, _state: A, _status: Result<(), Need>) {}
}
