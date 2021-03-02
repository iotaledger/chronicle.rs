use super::*;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

pub trait Appsthrough<B: ThroughType>: for<'de> Deserialize<'de> + Serialize + Send + 'static {
    fn is_self(&self, my_app_name: &str) -> bool {
        self.get_app_name() == my_app_name
    }
    fn get_app_name(&self) -> &str;
    fn try_get_my_event(self) -> Result<B::Through, Self>;
}

pub trait LauncherSender<B: ThroughType + Builder>: Send + Clone + 'static + AknShutdown<<B as Builder>::State> {
    type Through;
    type AppsEvents: Appsthrough<B>;
    fn start_app(&mut self, app_name: &str);
    fn shutdown_app(&mut self, app_name: &str);
    fn request_service(&mut self);
    /// Keep the launcher up to date with service status changes,
    /// NOTE: don't use it to send ServiceStatus::Stopped as it will be set automatically when app aknowledge_shutdown
    fn status_change(&mut self, service: Service);
    fn passthrough(&mut self, event: Self::AppsEvents, from_app_name: String);
    fn exit_program(&mut self, using_ctrl_c: bool);
}

/// The possible statuses a service (application) can be
#[repr(u8)]
#[derive(Clone, PartialEq, Debug, Serialize)]
pub enum ServiceStatus {
    /// Early bootup
    Starting = 0,
    /// Late bootup
    Initializing = 1,
    /// The service is operational but one or more services failed(Degraded or Maintenance) or in process of being
    /// fully operational while startup.
    Degraded = 2,
    /// The service is fully operational.
    Running = 3,
    /// The service is shutting down, should be handled accordingly by active dependent services
    Stopping = 4,
    /// The service is maintenance mode, should be handled accordingly by active dependent services
    Maintenance = 5,
    /// The service is not running, should be handled accordingly by active dependent services
    Stopped = 6,
}

/// An application's metrics
#[derive(Clone, Debug, Serialize)]
pub struct Service {
    /// The status of the service
    pub status: ServiceStatus,
    /// Service name (ie app name or microservice name)
    pub name: String,
    /// Timestamp since the service is up
    pub up_since: SystemTime,
    /// Total milliseconds when the app service has been offline since up_since
    pub downtime_ms: u64,
    /// inner services similar
    pub microservices: std::collections::HashMap<String, Self>,
    /// Optional log file path
    pub log_path: Option<String>,
}

impl Service {
    /// Create a new Service
    pub fn new() -> Self {
        Self {
            status: ServiceStatus::Starting,
            name: String::new(),
            up_since: SystemTime::now(),
            downtime_ms: 0,
            microservices: std::collections::HashMap::new(),
            log_path: None,
        }
    }
    /// Set the service status
    pub fn set_status(mut self, service_status: ServiceStatus) -> Self {
        self.status = service_status;
        self
    }
    /// Update the service status
    pub fn update_status(&mut self, service_status: ServiceStatus) {
        self.status = service_status;
    }
    /// Set the service (application) name
    pub fn set_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }
    /// Update the service (application) name
    pub fn update_name(&mut self, name: String) {
        self.name = name;
    }
    /// Get the service (application) name
    pub fn get_name(&self) -> String {
        self.name.clone()
    }
    /// Set the service downtime in milliseconds
    pub fn set_downtime_ms(mut self, downtime_ms: u64) -> Self {
        self.downtime_ms = downtime_ms;
        self
    }
    /// Set the logging filepath
    pub fn set_log(mut self, log_path: String) -> Self {
        self.log_path = Some(log_path);
        self
    }
    /// Insert a new microservice
    pub fn update_microservice(&mut self, service_name: String, microservice: Self) {
        self.microservices.insert(service_name, microservice);
    }
    /// Update the status of a microservice
    pub fn update_microservice_status(&mut self, service_name: &str, status: ServiceStatus) {
        self.microservices.get_mut(service_name).unwrap().status = status;
    }
    /// Delete a microservice
    pub fn delete_microservice(&mut self, service_name: &str) {
        self.microservices.remove(service_name);
    }
    /// Check if the service is stopping
    pub fn is_stopping(&self) -> bool {
        self.status == ServiceStatus::Stopping
    }
    /// Check if the service is stopped
    pub fn is_stopped(&self) -> bool {
        self.status == ServiceStatus::Stopped
    }
    /// Check if the service is running
    pub fn is_running(&self) -> bool {
        self.status == ServiceStatus::Running
    }
    /// Check if the service is starting
    pub fn is_starting(&self) -> bool {
        self.status == ServiceStatus::Starting
    }
    /// Check if the service is initializing
    pub fn is_initializing(&self) -> bool {
        self.status == ServiceStatus::Initializing
    }
    /// Check if the service is in maintenance
    pub fn is_maintenance(&self) -> bool {
        self.status == ServiceStatus::Maintenance
    }
    /// Check if the service is degraded
    pub fn is_degraded(&self) -> bool {
        self.status == ServiceStatus::Degraded
    }
    /// Get the service status
    pub fn service_status(&self) -> &ServiceStatus {
        &self.status
    }
}

/// Creates a launcher application and its managed apps
#[macro_export]
macro_rules! launcher {
    // useful to count how many field in a struct at the compile time.
    (@count $t1:tt, $($t:tt),+) => { 1 + launcher!(@count $($t),+) };
    (@count $t:tt) => { 1 };
    (
        builder: $name:ident {$( [$($dep:ty),*] -> $app:ident $(<$($a:tt),*>)? : $app_builder:tt$(<$($i:tt),*>)?  ),+},
        state: $apps:ident {$($field:ident : $type:ty),*}
    ) => {
        use std::collections::VecDeque;
        use std::future::Future;
        use tokio::sync::mpsc;
        use log::*;
        use serde::{
            Deserialize,
            Serialize,
        };

        const APPS: [&str; launcher!(@count $($app),+)] = [$(stringify!($app)),*];
        #[allow(missing_docs)]
        /// AppsEvents identify the applications specs
        /// AppsEvents used to identify the socket msg
        #[derive(Deserialize, Serialize)]
        pub enum AppsEvents {
            $(
                $app(<$app_builder$(<$($i,)*>)? as ThroughType>::Through),
            )*
        }
        #[allow(missing_docs)]
        /// All applications defined by this launcher
        pub enum AppsStates {
            $(
                $app(<$app_builder$(<$($i,)*>)? as Starter<Sender>>::Input),
            )*
        }

        #[derive(Default)]
        #[allow(non_snake_case)]
        /// Application handlers defined by this launcher
        pub struct AppsHandlers {
            $(
                $app: Option<<$app_builder$(<$($i,)*>)? as Starter<Sender>>::Ok>,
            )*
        }

        #[derive(Default)]
        #[allow(non_snake_case)]
        /// Application dependencies defined by this launcher
        pub struct AppsDeps {
            $(
                /// List of applications names that depend on $app
                $app: Vec<String>,
            )*
        }

        /// Global event types
        pub enum Event {
            /// Start an application
            StartApp(String, Option<AppsStates>),
            /// Shutdown an application
            ShutdownApp(String),
            /// Acknowledge an application shutdown
            AknShutdown(AppsStates, Result<(), Need>),
            /// Request an application's status
            RequestService(String),
            /// Notify a status change
            StatusChange(Service),
            /// Passthrough event
            Passthrough(AppsEvents, String),
            /// ExitProgram(using_ctrl_c: bool) using_ctrl_c will identify if the shutdown signal was initiated by ctrl_c
            ExitProgram {
                /// Did this exit program event happen because of a ctrl-c?
                using_ctrl_c: bool,
            },
        }

        /// Sender half of an event channel
        #[derive(Clone)]
        pub struct Sender(mpsc::UnboundedSender<Event>);
        /// Receiver half of an event channel
        pub struct Receiver(mpsc::UnboundedReceiver<Event>);

        /// Defines a builder type for this launcher
        pub trait LauncherBuilder {
            /// Builder type
            type Builder: Builder;
        }

        // auto implementations
        impl LauncherBuilder for $name {
            type Builder = $name;
        }

        $(
            impl LauncherSender<$app_builder$(<$($i,)*>)?> for Sender {
                type Through = <$app_builder$(<$($i,)*>)? as ThroughType>::Through;
                type AppsEvents = AppsEvents;
                fn start_app(&mut self, app_name: &str) {
                    let start_app_event = Event::StartApp(app_name.to_string(), None);
                    let _ = self.0.send(start_app_event);
                }
                fn shutdown_app(&mut self, app_name: &str) {
                    let shutdown_app_event = Event::ShutdownApp(app_name.to_string());
                    let _ = self.0.send(shutdown_app_event);
                }
                fn request_service(&mut self) {
                    let apps_status_event = Event::RequestService(stringify!($app).to_string());
                    let _ = self.0.send(apps_status_event);
                }
                fn status_change(&mut self, service: Service) {
                    let status_change_event = Event::StatusChange(service);
                    let _ = self.0.send(status_change_event);
                }
                fn passthrough(&mut self, event: AppsEvents, from_app_name: String) {
                    let apps_event = Event::Passthrough(event, from_app_name);
                    let _ = self.0.send(apps_event);
                }
                fn exit_program(&mut self, using_ctrl_c: bool) {
                    let exit_program_event = Event::ExitProgram { using_ctrl_c };
                    let _ = self.0.send(exit_program_event);
                }
            }

            impl Appsthrough<$app_builder$(<$($i,)*>)?> for AppsEvents {
                fn get_app_name(&self) -> &str {
                    stringify!($app)
                }
                #[allow(unreachable_patterns)]
                fn try_get_my_event(self) -> Result<<$app_builder$(<$($i,)*>)? as ThroughType>::Through, Self> {
                    match self {
                        AppsEvents::$app(e) => Ok(e),
                        _ => Err(self),
                    }
                }
            }

            #[async_trait::async_trait]
            impl AknShutdown< $app$( <$($a,)*> )? > for Sender {
                async fn aknowledge_shutdown(self, state: $app$(<$($a,)*>)?, status: Result<(), Need>) {
                    let input = From::< $app$(<$($a,)*>)? >::from(state);
                    let aknowledge_shutdown_event = Event::AknShutdown(AppsStates::$app(input), status);
                    let _ = self.0.send(aknowledge_shutdown_event);
                }
            }
        )*
        /// Launcher builder
        #[derive(Clone, Default)]
        #[allow(non_snake_case)]
        pub struct $name {
            $(
                $app: Option<$app_builder$(<$($i,)*>)?>,
            )*
            $(
                $field: Option<$type>,
            )*
        }
        impl $name {
            /// Instantiate a launcher
            pub fn new() -> Self {
                Self {
                    $(
                        $app: None,
                    )*
                    $(
                        $field: None,
                    )*
                }
            }
            $(
                pub fn $field(mut self, $field: $type) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
            /// Consume the launcher and emit its apps
            pub fn to_apps(self) -> $apps {
                let service = Service::new();
                let apps_names = vec![ $(stringify!($app).to_string()),*];
                let apps_handlers = AppsHandlers::default();
                let mut apps_deps: AppsDeps = AppsDeps::default();
                $(
                    $(
                        let dep = stringify!($dep);
                        if APPS.iter().any(|&x| x == dep) {
                            apps_deps.$app.push(dep.to_string());
                        } else {
                            panic!("{} dependency in {} doesn't exist as application", dep, stringify!($app));
                        }
                    )*
                    apps_deps.$app.reverse();
                )*
                let shutdown_queue: VecDeque<String> = VecDeque::new();
                let app_count: usize = launcher!(@count $($app),+);
                let (tx, rx) = mpsc::unbounded_channel::<Event>();
                let consumed_ctrl_c = false;
                $apps {
                    service,
                    apps_names,
                    apps_handlers,
                    apps_deps,
                    shutdown_queue,
                    app_count,
                    tx: Sender(tx),
                    rx: Receiver(rx),
                    consumed_ctrl_c,
                    $(
                        $app: self.$app,
                    )*
                    $(
                        $field: self.$field.unwrap(),
                    )*
                }.set_name()
            }

            $(
                /// Access the $app application
                #[allow(non_snake_case)]
                pub fn $app(mut self, $app_builder: $app_builder$(<$($i,)*>)?) -> Self {
                    self.$app.replace($app_builder);
                    self
                }
            )*

        }
        /// Launcher state
        #[allow(non_snake_case)]
        pub struct $apps {
            service: Service,
            apps_names: Vec<String>,
            apps_handlers: AppsHandlers,
            apps_deps: AppsDeps,
            shutdown_queue: VecDeque<String>,
            app_count: usize,
            tx: Sender,
            rx: Receiver,
            $(
                $app: Option<$app_builder$(<$($i,)*>)?>,
            )*
            $(
                $field: $type,
            )*
            consumed_ctrl_c: bool,
        }
        /// Launcher state implementation
        #[allow(dead_code)]
        impl Apps {
            /// Helper function to call a synchronous function with $apps
            pub async fn function(mut self, f: impl Fn(&mut $apps)) -> Self {
                f(&mut self);
                self
            }
            /// Helper function to call an asynchronous function with $apps
            pub async fn future<F>(self, f: impl Fn($apps) -> F) -> Self
            where F: Future<Output=$apps>
            {
                f(self).await
            }
            $(
                #[allow(non_snake_case)]
                /// Initial start
                async fn $app(mut self) -> Self {
                    let app_name = stringify!($app).to_owned();
                    let app_handle = self.$app.clone().take().unwrap()
                        .starter(self.tx.clone(), None)
                        .await;
                    match app_handle {
                        Ok(h) => {
                            self.apps_handlers.$app.replace(h);
                            // create new service that indicates the app is starting
                            let service = Service::new().set_name(app_name.clone());
                            // update main service
                            self.service.update_microservice(app_name, service);
                            info!("Succesfully starting {}", stringify!($app));
                        }
                        Err(e) => {
                            // create new service that indicates the app is stopped
                            let service = Service::new().set_status(ServiceStatus::Stopped).set_name(app_name.clone());
                            // update main service
                            self.service.update_microservice(app_name, service);
                            error!("Unable to start {}: {}", stringify!($app), e);
                        }
                    }
                    self
                }
            )*
            // cast status_change to all active applications
            fn status_change(&mut self, service: Service) {
                $(
                    if let Some(app_handler) = self.apps_handlers.$app.as_mut() {
                        app_handler.app_status_change(&service);
                    }
                )*
                // update the service for microservice
                self.service.update_microservice(service.name.clone(), service);
            }
            // cast launcher status_change to all active applications without updating self.service
            fn launcher_status_change(&mut self) {
                $(
                    if let Some(app_handler) = self.apps_handlers.$app.as_mut() {
                        app_handler.launcher_status_change(&self.service);
                    }
                )*
            }
            fn shutdown_app(&mut self, app_name: String) {
                match &app_name[..] {
                    $(
                        stringify!($app) => {

                            if let Some(app_handler) = self.apps_handlers.$app.take() {
                                self.shutdown_queue.push_front(app_name.clone());
                                // make sure app deps to get shutdown first
                                for app in &self.apps_deps.$app {
                                    // add only if the app not stopped
                                    let is_dep_stopped = self.service.microservices.get(&app[..]).unwrap().is_stopped();
                                    if !is_dep_stopped {
                                        // shutdown the dep if not already stopped
                                        self.shutdown_queue.push_front(app.clone());
                                    }
                                }
                                // shutdown in order
                                let shutdown_first = self.shutdown_queue.pop_front().unwrap();
                                // if shutdown_first == app_name then all deps of app_name are already stopped
                                if shutdown_first == app_name {
                                    // try to init shutdown signal in meantime
                                    if let Some(app_handler) = app_handler.shutdown() {
                                        error!("unable to init shutdown signal for {}", app_name);
                                        self.apps_handlers.$app.replace(app_handler);
                                        // spawn another ctrl_c if consumed_ctrl_c == true
                                        if self.consumed_ctrl_c {
                                            tokio::spawn(ctrl_c(self.tx.clone()));
                                            // reset consumed_ctrl_c
                                            self.consumed_ctrl_c = false;
                                        }
                                        if self.service.is_stopping() {
                                            error!(
                                                "unable to exit program, please try again, using dashboard or from shell `kill -SIGINT {}`",
                                                std::process::id()
                                            );
                                        }
                                    // TODO log to main service
                                    } else {
                                        // succesfully initiated shutdown signal
                                        info!("initiated shutdown signal for {}", app_name);
                                    };
                                } else {
                                    // return the handler, because it's not yet ready to get shutdown
                                    self.apps_handlers.$app.replace(app_handler);
                                    self.shutdown_app(shutdown_first.clone())
                                }
                            } else {
                                // get the app_status
                                let app_status = &self.service.microservices.get(&app_name[..]).unwrap().status;

                                if app_status == &ServiceStatus::Stopped {
                                    info!("no app handler for {}, as it's already stopped", app_name);
                                    // shutdown next app (if any)
                                    if let Some(app) = self.shutdown_queue.pop_front() {
                                        self.shutdown_app(app);
                                    } else {
                                        // nothing to shutdown, check if main service is stopping to send exit_program
                                        if self.service.is_stopping() {
                                            let exit_program_event = Event::ExitProgram { using_ctrl_c: false };
                                            let _ = self.tx.0.send(exit_program_event);
                                        }
                                    }
                                } else {
                                    warn!(
                                        "no app handler for {}, but it's already in process to get stopped, current service status: {:?}",
                                        app_name, app_status
                                    );
                                }
                                if self.consumed_ctrl_c {
                                    tokio::spawn(ctrl_c(self.tx.clone()));
                                    // reset consumed_ctrl_c
                                    self.consumed_ctrl_c = false;
                                }
                            }
                        }
                    )*
                    _ => error!("cannot shutdown app: {}, as it doesn't exist", app_name),

                }
            }
            /// dynamically start an application
            async fn start_app(&mut self, app_name: String, apps_states_opt: Option<AppsStates>) {
                if !self.service.is_stopping() {
                    match &app_name[..] {
                        $(
                            stringify!($app) => {
                                // get the app_status
                                let app_status = &self.service.microservices.get(&app_name[..]).unwrap().status;
                                // check if app is stopped to make sure is safe to start it;
                                if app_status == &ServiceStatus::Stopped {
                                    // get app_state (if any)
                                    let app_state_opt;
                                    if let Some(AppsStates::$app(app_state)) = apps_states_opt {
                                        app_state_opt = Some(app_state);
                                    } else {
                                        app_state_opt = None;
                                    }
                                    let app_handle = self.$app.clone().take().unwrap()
                                        .starter(self.tx.clone(), app_state_opt)
                                        .await;
                                    match app_handle {
                                        Ok(app_handler) => {
                                            // store the app_handler
                                            self.apps_handlers.$app.replace(app_handler);
                                            // create new service that indicates the app is starting
                                            let service = Service::new().set_name(app_name.clone());
                                            // store service in the apps_status
                                            self.service.update_microservice(app_name.clone(), service.clone());
                                            info!("starter succesfully started {}", app_name);
                                            // tell active applications (including app_name)
                                            self.status_change(service);
                                        }
                                        Err(_err) => {
                                            // create new service that indicates the app is stopped
                                            let service = Service::new().set_status(ServiceStatus::Stopped).set_name(app_name.clone());
                                            // store it in the apps_status
                                            self.service.update_microservice(app_name.clone(), service.clone());
                                            error!("starter unable to start {}", app_name);
                                            // tell active applications
                                            self.status_change(service);
                                        }
                                    }
                                } else {
                                    warn!("cannot starts {}, because already {:?}", app_name, app_status);
                                }
                            }
                        )*
                        _ => error!("cannot start app: {}, as it doesn't exist", app_name),

                    }
                } else {
                    // Apps is stopping, and we shouldn't start any application durring this state,
                    warn!("cannot starts {}, because {} is stopping", app_name, self.get_name());
                }
            }
        }

        /// Name of the launcher
        impl Name for $apps {
            fn get_name(&self) -> String {
                self.service.name.clone()
            }
            fn set_name(mut self) -> Self {
                self.service.update_name(stringify!($apps).to_string().to_string());
                self
            }
        }

        #[async_trait::async_trait]
        impl Init<NullSupervisor> for $apps {
            async fn init(&mut self, status: Result<(), Need>, _supervisor: &mut Option<NullSupervisor>) -> Result<(), Need> {
                tokio::spawn(ctrl_c(self.tx.clone()));
                info!("Initializing {} Apps", self.app_count);
                // update service to be Initializing
                self.service.update_status(ServiceStatus::Initializing);
                // tell active apps
                self.launcher_status_change();
                status
            }
        }

        #[async_trait::async_trait]
        impl EventLoop<NullSupervisor> for $apps {
            async fn event_loop(&mut self, status: Result<(), Need>, _supervisor: &mut Option<NullSupervisor>) -> Result<(), Need> {
                // update service to be Running
                self.service.update_status(ServiceStatus::Running);
                // tell active apps
                self.launcher_status_change();
                while let Some(event) = self.rx.0.recv().await {
                    match event {
                        Event::StartApp(app_name, apps_states_opt) => {
                            info!("trying to start {}", app_name);
                            self.start_app(app_name, apps_states_opt).await;
                        }
                        Event::ShutdownApp(app_name) => {
                            info!("trying to shutdown {}", app_name);
                            self.shutdown_app(app_name);
                        }
                        Event::StatusChange(service) => {
                            info!("{} is {:?}, telling all active applications", service.name, service.status,);
                            self.status_change(service);
                        }
                        Event::AknShutdown(app_states, result) => {
                            // shutdown next app
                            match app_states {
                                //$apps
                                $(
                                    AppsStates::$app(ref _state_opt) => {
                                        let app_name = stringify!($app);
                                        self.service.update_microservice_status(app_name, ServiceStatus::Stopped);
                                        // tell all active application with this change
                                        let service = self.service.microservices.get(&app_name[..]).unwrap().clone();
                                        info!(
                                            "{} is stopped, acknowledging shutdown and telling all active applications",
                                            service.name,
                                        );
                                        self.status_change(service);
                                        // drop handler if any
                                        let app_handler_opt = self.apps_handlers.$app.take();
                                        // check if the app needs restart/kill or RescheduleAfter
                                        match result {
                                            Err(Need::RescheduleAfter(after_duration)) => {
                                                // reschedule a restart after duration
                                                info!(
                                                    "rescheduling {} restart after duration {} milliseconds",
                                                    app_name,
                                                    after_duration.as_millis(),
                                                );
                                                // clone sender tx
                                                let launcher_tx = self.tx.clone();
                                                // schedule the restart
                                                tokio::spawn(async move {
                                                    tokio::time::sleep(after_duration).await;
                                                    info!("rescheduled duration has passed for {}", app_name);
                                                    // this will enable the app to ask for restart after the delay pass
                                                    let _ = launcher_tx.0.send(Event::StartApp(app_name.to_string(), Some(app_states)));
                                                });
                                            }
                                            Err(Need::Restart) => {
                                                if app_handler_opt.is_some() {
                                                    // the application asking for restart
                                                    info!("{} restarting itself", app_name);
                                                    // restart the application
                                                    self.start_app(app_name.to_string(), Some(app_states)).await;
                                                } else {
                                                    warn!("cannot restart {}, as we asked to shutdown", app_name);
                                                }
                                            }
                                            Err(Need::Abort) => {
                                                if app_handler_opt.is_some() {
                                                    // the application is aborting
                                                    info!("{} aborted itself", app_name)
                                                } else {
                                                    info!("aborted shutdown {}", app_name);
                                                }
                                            }
                                            Ok(()) => {
                                                if app_handler_opt.is_some() {
                                                    // gracefully the application shutdown itself
                                                    info!("{} gracefully shutdown itself", app_name)
                                                } else {
                                                    info!("gracefully shutdown {}", app_name);
                                                }
                                            }
                                        }
                                        // shutdown next app (if any) only if is not already stopping
                                        if let Some(app) = self.shutdown_queue.pop_front() {
                                            if !self.service.microservices.get(&app[..]).unwrap().is_stopping() {
                                                self.shutdown_app(app);
                                            } else {
                                                self.shutdown_queue.push_front(app);
                                            };
                                        } else if self.service.is_stopping() {
                                            // keep exiting
                                            let exit_program_event = Event::ExitProgram { using_ctrl_c: false };
                                            let _ = self.tx.0.send(exit_program_event);
                                        }
                                    }
                                )*

                            }
                        }
                        Event::Passthrough(apps_event, from_app_name) => {
                            match apps_event {
                                $(
                                    AppsEvents::$app(event) => {
                                        if let Some(app_handler) = self.apps_handlers.$app.as_mut() {
                                            app_handler.passthrough(event, from_app_name)
                                        }
                                    },
                                )*
                            }
                        },
                        Event::ExitProgram { using_ctrl_c } => {
                            if !self.service.is_stopping() {
                                info!("Exiting Program in progress");
                                // update service to be Stopping
                                self.service.update_status(ServiceStatus::Stopping);
                                // tell active apps
                                self.launcher_status_change();
                            }
                            // identfiy if ctrl_c has been consumed
                            if using_ctrl_c {
                                self.consumed_ctrl_c = true;
                            }
                            if let Some(app_name) = self.apps_names.last() {
                                let app_name = app_name.to_string();
                                // check if app is not already stopped
                                let app_status = &self.service.microservices.get(&app_name[..]).unwrap().status;
                                if app_status == &ServiceStatus::Stopped {
                                    // pop the app as it is already stopped
                                    self.apps_names.pop();
                                    // send another ExitProgram
                                    let exit_program_event = Event::ExitProgram { using_ctrl_c: false };
                                    let _ = self.tx.0.send(exit_program_event);
                                } else {
                                    info!("trying to shutdown {}", app_name);
                                    self.shutdown_app(app_name);
                                };
                            } else {
                                info!("Aknowledged shutdown for all Apps",);
                                break;
                            }
                        }
                        Event::RequestService(requester_app_name) => {
                            match &requester_app_name[..] {
                                $(
                                    stringify!($app) => {
                                        if let Some(requester) = self.apps_handlers.$app.as_mut() {
                                            requester.service(&self.service);
                                        }
                                    },
                                )*
                                _ => {
                                    error!("invalid requester app name");
                                }
                            }

                        }
                    }
                }
                status
            }
        }

        #[async_trait::async_trait]
        impl Terminating<NullSupervisor> for $apps {
            async fn terminating(&mut self, mut status: Result<(), Need>, _supervisor: &mut Option<NullSupervisor>) -> Result<(), Need> {
                if let Err(Need::Abort) = status {
                    let exit_program_event = Event::ExitProgram { using_ctrl_c: false };
                    let _ = self.tx.0.send(exit_program_event);
                    status = self.event_loop(status, _supervisor).await;
                }
                // update service to stopped
                self.service.update_status(ServiceStatus::Stopped);
                info!("Thank you for using Chronicle.rs, GoodBye");
                status
            }
        }

        /// Useful function to exit program using ctrl_c signal
        async fn ctrl_c(mut handle: Sender) {
            // await on ctrl_c
            tokio::signal::ctrl_c().await.unwrap();
            // exit program using launcher
            let exit_program_event = Event::ExitProgram { using_ctrl_c: true };
            let _ = handle.0.send(exit_program_event);
        }
    };

}
