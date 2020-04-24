use std::collections::HashMap;
pub trait DashboardTx: Send {
    fn started_app(&mut self, app_name: String);
    fn shutdown_app(&mut self, app_name: String);
    fn apps_status(&mut self, apps_status: HashMap<String, AppStatus>);
}
// apps status enum
pub enum AppStatus {
    Running(String),
    Shutdown(String),
    ShuttingDown(String),
}
