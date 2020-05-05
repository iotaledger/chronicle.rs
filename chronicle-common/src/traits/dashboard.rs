use std::collections::HashMap;
pub trait DashboardTx: Send {
    fn starting_app(&mut self, app_name: String);
    fn started_app(&mut self, app_name: String);
    fn restarted_app(&mut self, app_name: String);
    fn shutdown_app(&mut self, app_name: String);
    fn apps_status(&mut self, apps_status: HashMap<String, AppStatus>);
}

pub type AppsStatus = HashMap<String, AppStatus>;

#[derive(Clone)]
pub enum AppStatus {
    Starting(String),
    Running(String),
    Shutdown(String),
    ShuttingDown(String),
    Restarting(String),
}
impl PartialEq for AppStatus {
    fn eq(&self, other: &AppStatus) -> bool {
        match (self, other) {
            (&AppStatus::Starting(_), &AppStatus::Starting(_)) => true,
            (&AppStatus::Running(_), &AppStatus::Running(_)) => true,
            (&AppStatus::Shutdown(_), &AppStatus::Shutdown(_)) => true,
            (&AppStatus::ShuttingDown(_), &AppStatus::ShuttingDown(_)) => true,
            (&AppStatus::Restarting(_), &AppStatus::Restarting(_)) => true,
            _ => false,
        }
    }
}
