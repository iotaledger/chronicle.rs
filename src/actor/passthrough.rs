use super::launcher::Service;

pub trait Passthrough<T>: Send {
    /// Here we recv events passed to us from passthrough
    fn send_event(&mut self, event: T, from_app_name: String);
    /// Tells apps about status_change for a given app
    /// this method will be invoked when any application broadcast status change in its service
    fn app_status_change(&mut self, service: &Service);
    /// Tells apps about status_change in the launcher/main service,
    /// this method will be invoked when the launcher wants to broadcast status change
    fn launcher_status_change(&mut self, service: &Service);
    /// Provides the requested main service,
    /// this method will be invoked only when the application request_service()
    fn service(&mut self, service: &Service);
}
