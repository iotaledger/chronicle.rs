use async_trait::async_trait;
use chronicle::*;

// App builder
builder!(
    #[derive(Clone)]
    HelloWorldBuilder<T> {}
);

impl<H: LauncherSender<Self>> ThroughType for HelloWorldBuilder<H> {
    type Through = HelloWorldEvent;
}

impl<H: LauncherSender<Self>> Builder for HelloWorldBuilder<H> {
    type State = HelloWorld;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<HelloWorldEvent>();
        let mut service = Service::new();
        service.update_name("HelloWorld".to_string());
        HelloWorld { tx, rx, service }
    }
}

impl<H: LauncherSender<Self>> AppBuilder<H> for HelloWorldBuilder<H> {}

impl Passthrough<HelloWorldEvent> for HelloWorldSender {
    fn passthrough(&mut self, _event: HelloWorldEvent, _from_app_name: String) {}
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
impl<H: LauncherSender<Self>> Starter<H> for HelloWorldBuilder<H> {
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
        tokio::spawn(Actor::start(hello_world, Some(handle)));
        // return app_handle
        Ok(app_handle)
    }
}

#[async_trait]
impl<H> Actor<H> for HelloWorld
where
    H: LauncherSender<HelloWorldBuilder<H>>,
{
    fn name(&self) -> String {
        self.service.get_name()
    }

    async fn init(&mut self, supervisor: &mut Option<H>) -> NeedResult {
        // update service to be Initializing
        self.service.update_status(ServiceStatus::Initializing);
        // tell active apps
        supervisor.as_mut().unwrap().status_change(self.service.clone());
        Ok(())
    }

    async fn event_loop(&mut self, supervisor: &mut Option<H>) -> NeedResult {
        // update service to be Running
        self.service.update_status(ServiceStatus::Running);
        // tell active apps
        supervisor.as_mut().unwrap().status_change(self.service.clone());
        // this scope is an example of how the application can make use of Appsthrough,
        // it's meant to be used to dynamically re-configure the applications during runtime
        {
            let apps_through: Result<H::AppsEvents, _> = serde_json::from_str("{\"HelloWorld\": \"Shutdown\"}");
            if let Ok(apps_events) = apps_through {
                // check if the event meant to be sent to HelloWorld application
                match apps_events.try_get_my_event() {
                    // event belong to self application
                    Ok(HelloWorldEvent::Shutdown) => {
                        supervisor.as_mut().unwrap().shutdown_app(<Self as Actor<H>>::name(self));
                    }
                    // event belongs to other application, so we passthrough to the launcher in order to route it
                    // to the corresponding application
                    Err(other_app_event) => {
                        supervisor
                            .as_mut()
                            .unwrap()
                            .passthrough(other_app_event, <Self as Actor<H>>::name(self));
                    }
                }
            } else {
                return Err(Need::Abort(AbortType::Error));
            };
        }
        while let Some(HelloWorldEvent::Shutdown) = self.rx.recv().await {
            // break the application event loop when it receives Shutdown event
            self.rx.close();
            // or break; NOTE: the application must make sure to shutdown all of its children
        }
        Ok(())
    }

    async fn terminating(&mut self, status: ActorResult, supervisor: &mut Option<H>) -> NeedResult {
        // update service to be Stopping
        self.service.update_status(ServiceStatus::Stopping);
        // tell active apps
        supervisor.as_mut().unwrap().status_change(self.service.clone());
        Ok(())
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

// launcher
launcher!(builder: AppsBuilder {[] -> HelloWorld: HelloWorldBuilder<Sender>}, state: Apps {});

impl Builder for AppsBuilder {
    type State = Apps;
    fn build(self) -> Self::State {
        // create app
        let hello_world_builder = HelloWorldBuilder::new();
        // add it to launcher
        self.HelloWorld(hello_world_builder).to_apps()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    // create apps_builder and build apps
    let apps = AppsBuilder::new().build();
    // start the launcher
    Actor::start(apps.HelloWorld().await, None).await;
}
