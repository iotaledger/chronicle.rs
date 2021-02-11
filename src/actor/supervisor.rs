use async_trait::async_trait;
use std::time::Duration;

use crate::ActorResult;

#[derive(Copy, Clone)]
pub enum Need {
    Restart,
    RescheduleAfter(Duration),
    Abort(AbortType),
}

#[derive(Copy, Clone)]
pub enum AbortType {
    Error,
    Timeout,
    Manual,
}

/// Should be implemented on the Actor(or pid) that should become a child
pub trait Child<H, S>: Send {
    fn handle_shutdown(self, handle: Option<H>, supervisor_state: &mut S)
    where
        Self: Sized;
}

/// Should be implemented on the supervisor_handle
#[async_trait]
pub trait AcknowledgeShutdown<A>: Send {
    async fn acknowledge_shutdown(self, state: A, status: ActorResult);
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
impl<A: Send + 'static> AcknowledgeShutdown<A> for NullSupervisor {
    async fn acknowledge_shutdown(self, state: A, status: ActorResult) {}
}
