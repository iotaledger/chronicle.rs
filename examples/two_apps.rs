//////////////////////////////// HelloWorld App ////////////////////////////////////////////
use async_trait::async_trait;
use chronicle::*;

// App builder
builder!(
    #[derive(Clone)]
    HelloWorldBuilder {}
);

impl ThroughType for HelloWorldBuilder {
    type Through = HelloWorldEvent;
}

impl Builder for HelloWorldBuilder {
    type State = HelloWorld;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<HelloWorldEvent>();
        HelloWorld {
            tx,
            rx,
            service: Service::new(),
        }
        .set_name()
    }
}

impl<H: LauncherSender<HelloWorldBuilder>> AppBuilder<H> for HelloWorldBuilder {}

impl<H: LauncherSender<HelloWorldBuilder>> Actor<H> for HelloWorld {}

impl Name for HelloWorld {
    fn get_name(&self) -> String {
        self.service.get_name()
    }
    fn set_name(mut self) -> Self {
        self.service.update_name("HelloWorld".to_string());
        self
    }
}

impl Passthrough<HelloWorldEvent> for HelloWorldSender {
    fn send_event(&mut self, _event: HelloWorldEvent, _from_app_name: String) {}
    fn service(&mut self, _service: &Service) {}
    fn launcher_status_change(&mut self, _service: &Service) {}
    fn app_status_change(&mut self, _service: &Service) {}
}

impl Shutdown for HelloWorldSender {
    fn shutdown(self) -> Option<Self> {
        let _ = self.tx.send(HelloWorldEvent::Shutdown);
        None
    }
}

#[async_trait]
impl<H: LauncherSender<Self>> Starter<H> for HelloWorldBuilder {
    type Ok = HelloWorldSender;
    type Error = ();
    // if application asked for Need::Restart or RescheduleAfter then the input will hold the prev app From::from(state)
    type Input = HelloWorld;
    async fn starter(mut self, handle: H, mut _input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        let hello_world = self.build();
        // create handle
        let app_handle = HelloWorldSender {
            tx: hello_world.tx.clone(),
        };
        // spawn and start HelloWorld
        tokio::spawn(hello_world.start(Some(handle)));
        // return app_handle
        Ok(app_handle)
    }
}

#[async_trait]
impl<H: LauncherSender<HelloWorldBuilder>> Init<H> for HelloWorld {
    async fn init(&mut self, status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        // update service to be Initializing
        self.service.update_status(ServiceStatus::Initializing);
        // tell active apps
        _supervisor.as_mut().unwrap().status_change(self.service.clone());
        status
    }
}

#[async_trait]
impl<H: LauncherSender<HelloWorldBuilder>> EventLoop<H> for HelloWorld {
    async fn event_loop(&mut self, _status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        // update service to be Running
        self.service.update_status(ServiceStatus::Running);
        // tell active apps
        _supervisor.as_mut().unwrap().status_change(self.service.clone());
        // this scope is an example of how the application can make use of Appsthrough,
        // it's meant to be used to dynamically re-configure the applications during runtime
        {
            let apps_through: Result<H::AppsEvents, _> = serde_json::from_str("{\"HelloWorld\": \"Shutdown\"}");
            if let Ok(apps_events) = apps_through {
                // check if the event meant to be sent to HelloWorld application
                match apps_events.try_get_my_event() {
                    // event belong to self application
                    Ok(HelloWorldEvent::Shutdown) => {
                        _supervisor.as_mut().unwrap().shutdown_app(&self.get_name());
                    }
                    // event belongs to other application, so we passthrough to the launcher in order to route it
                    // to the corresponding application
                    Err(other_app_event) => {
                        _supervisor.as_mut().unwrap().passthrough(other_app_event, self.get_name());
                    }
                }
            } else {
                return Err(Need::Abort);
            };
        }
        while let Some(HelloWorldEvent::Shutdown) = self.rx.recv().await {
            // break the application event loop when it receives Shutdown event
            self.rx.close();
            // or break; NOTE: the application must make sure to shutdown all of its children
        }
        _status
    }
}

#[async_trait]
impl<H: LauncherSender<HelloWorldBuilder>> Terminating<H> for HelloWorld {
    async fn terminating(&mut self, _status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        // update service to be Stopping
        self.service.update_status(ServiceStatus::Stopping);
        // tell active apps
        _supervisor.as_mut().unwrap().status_change(self.service.clone());
        _status
    }
}

/// App starter state and also the root state
pub struct HelloWorld {
    tx: tokio::sync::mpsc::UnboundedSender<HelloWorldEvent>,
    rx: tokio::sync::mpsc::UnboundedReceiver<HelloWorldEvent>,
    service: Service,
}

/// Root handler
#[allow(dead_code)]
pub struct HelloWorldSender {
    tx: tokio::sync::mpsc::UnboundedSender<HelloWorldEvent>,
}

/// Through type
#[derive(Serialize, Deserialize)]
pub enum HelloWorldEvent {
    Shutdown,
}

//////////////////////////////// Howdy App ////////////////////////////////////////////

// App builder
builder!(
    #[derive(Clone)]
    HowdyBuilder {}
);

impl ThroughType for HowdyBuilder {
    type Through = HowdyEvent;
}

impl Builder for HowdyBuilder {
    type State = Howdy;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<HowdyEvent>();
        Howdy {
            tx,
            rx,
            service: Service::new(),
        }
        .set_name()
    }
}

impl<H: LauncherSender<HowdyBuilder>> AppBuilder<H> for HowdyBuilder {}

impl<H: LauncherSender<HowdyBuilder>> Actor<H> for Howdy {}

impl Name for Howdy {
    fn get_name(&self) -> String {
        self.service.get_name()
    }
    fn set_name(mut self) -> Self {
        self.service.update_name("Howdy".to_string());
        self
    }
}

impl Passthrough<HowdyEvent> for HowdySender {
    fn send_event(&mut self, _event: HowdyEvent, _from_app_name: String) {}
    fn service(&mut self, _service: &Service) {}
    fn launcher_status_change(&mut self, _service: &Service) {}
    fn app_status_change(&mut self, _service: &Service) {}
}

impl Shutdown for HowdySender {
    fn shutdown(self) -> Option<Self> {
        let _ = self.tx.send(HowdyEvent::Shutdown);
        None
    }
}

#[async_trait]
impl<H: LauncherSender<HowdyBuilder>> Starter<H> for HowdyBuilder {
    type Ok = HowdySender;
    type Error = ();
    // if application asked for Need::Restart or RescheduleAfter then the input will hold the prev app From::from(state)
    type Input = Howdy;
    async fn starter(mut self, handle: H, mut _input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        let howdy = self.build();
        // create handle
        let app_handle = HowdySender { tx: howdy.tx.clone() };
        // spawn and start Howdy
        tokio::spawn(howdy.start(Some(handle)));
        // return app_handle
        Ok(app_handle)
    }
}

#[async_trait]
impl<H: LauncherSender<HowdyBuilder>> Init<H> for Howdy {
    async fn init(&mut self, status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        // update service to be Initializing
        self.service.update_status(ServiceStatus::Initializing);
        // tell active apps
        _supervisor.as_mut().unwrap().status_change(self.service.clone());
        status
    }
}

#[async_trait]
impl<H: LauncherSender<HowdyBuilder>> EventLoop<H> for Howdy {
    async fn event_loop(&mut self, _status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        // update service to be Running
        self.service.update_status(ServiceStatus::Running);
        // tell active apps
        _supervisor.as_mut().unwrap().status_change(self.service.clone());
        // this scope is an example of how the application can make use of Appsthrough,
        // it's meant to be used to dynamically re-configure the applications during runtime
        {
            let apps_through: Result<H::AppsEvents, _> = serde_json::from_str("{\"Howdy\": \"Shutdown\"}");
            if let Ok(apps_events) = apps_through {
                // check if the event meant to be sent to Howdy application
                match apps_events.try_get_my_event() {
                    // event belong to self application
                    Ok(HowdyEvent::Shutdown) => {
                        _supervisor.as_mut().unwrap().shutdown_app(&self.get_name());
                    }
                    // event belong to other application, so we passthrough to the launcher in order to route it
                    // to the corresponding application
                    Err(other_app_event) => {
                        _supervisor.as_mut().unwrap().passthrough(other_app_event, self.get_name());
                    }
                }
            } else {
                return Err(Need::Abort);
            };
        }
        while let Some(HowdyEvent::Shutdown) = self.rx.recv().await {
            // break the application event loop when it receives Shutdown event
            self.rx.close();
            // or break; NOTE: the application must make sure to shutdown all of its children
        }
        _status
    }
}

#[async_trait]
impl<H: LauncherSender<HowdyBuilder>> Terminating<H> for Howdy {
    async fn terminating(&mut self, _status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        // update service to be Stopping
        self.service.update_status(ServiceStatus::Stopping);
        // tell active apps
        _supervisor.as_mut().unwrap().status_change(self.service.clone());
        _status
    }
}

/// App starter state and also the root state
pub struct Howdy {
    tx: tokio::sync::mpsc::UnboundedSender<HowdyEvent>,
    rx: tokio::sync::mpsc::UnboundedReceiver<HowdyEvent>,
    service: Service,
}

/// Root handler
#[allow(dead_code)]
pub struct HowdySender {
    tx: tokio::sync::mpsc::UnboundedSender<HowdyEvent>,
}

/// Through type
#[derive(Serialize, Deserialize)]
pub enum HowdyEvent {
    Shutdown,
}

//////////////////////////////// Launcher (root of program) ////////////////////////////////////////////
launcher!(builder: AppsBuilder {[] -> HelloWorld: HelloWorldBuilder, [] -> Howdy: HowdyBuilder}, state: Apps {});

impl Builder for AppsBuilder {
    type State = Apps;
    fn build(self) -> Self::State {
        // create apps
        let hello_world_builder = HelloWorldBuilder::new();
        let howdy_builder = HowdyBuilder::new();
        // add it to launcher
        self.HelloWorld(hello_world_builder).Howdy(howdy_builder).to_apps()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    // create apps_builder and build apps
    let apps = AppsBuilder::new().build();
    // start the launcher
    apps.HelloWorld().await.Howdy().await.start(None).await;
}
