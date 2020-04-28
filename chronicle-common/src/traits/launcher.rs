use super::{
    shutdown::ShutdownTx,
    dashboard::DashboardTx,
};
pub trait LauncherTx: Send + LauncherTxClone {
    fn start_app(&mut self, app_name: String);
    fn shutdown_app(&mut self, app_name: String);
    fn aknowledge_shutdown(&mut self, app_name: String);
    fn register_dashboard(&mut self, dashboard_tx: Box<dyn DashboardTx>);
    fn register_app(&mut self, app_name: String, shutdown_tx: Box<dyn ShutdownTx>);
}

impl Clone for Box<dyn LauncherTx> {
    fn clone(&self) -> Box<dyn LauncherTx> {
        self.clone_box()
    }
}

pub trait LauncherTxClone {
    fn clone_box(&self) -> Box<dyn LauncherTx>;
}

impl<T> LauncherTxClone for T
where
    T: 'static + LauncherTx + Clone,
{
    fn clone_box(&self) -> Box<dyn LauncherTx> {
        Box::new(self.clone())
    }
}
