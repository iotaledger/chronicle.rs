pub trait ShutdownTx: Send {
    fn shutdown(self);
}
