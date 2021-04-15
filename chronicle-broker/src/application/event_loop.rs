use super::*;
use futures::SinkExt;
use chronicle_common::get_history_mut;

#[async_trait]
impl<H: ChronicleBrokerScope> EventLoop<H> for ChronicleBroker<H> {
    async fn event_loop(
        &mut self,
        mut _status: Result<(), Need>,
        supervisor: &mut Option<H>,
    ) -> Result<(), Need> {
        _status?;
        if let Some(ref mut supervisor) = supervisor {
            self.service.update_status(ServiceStatus::Running);
            while let Some(event) = self.inbox.recv().await {
                match event {
                    BrokerEvent::Passthrough(passthrough_events) => match passthrough_events.try_get_my_event() {
                        Ok(my_event) => match my_event {
                            ChronicleBrokerThrough::Shutdown => {
                                self.shutdown(supervisor, true).await;
                                // ensure to drop handle
                                self.handle.take();
                            }
                            ChronicleBrokerThrough::Topology(topology) => match topology {
                                Topology::AddMqttMessages(url) => {
                                    if let Some(mqtt) = self.add_mqtt(Messages, MqttType::Messages, url) {
                                        tokio::spawn(mqtt.start(self.handle.clone()));
                                    }
                                }
                                Topology::AddMqttMessagesReferenced(url) => {
                                    if let Some(mqtt) =
                                        self.add_mqtt(MessagesReferenced, MqttType::MessagesReferenced, url)
                                    {
                                        tokio::spawn(mqtt.start(self.handle.clone()));
                                    }
                                }
                                Topology::RemoveMqttMessagesReferenced(url) => {
                                    self.remove_mqtt::<MessagesReferenced>(MqttType::MessagesReferenced, url)
                                }
                                Topology::RemoveMqttMessages(url) => {
                                    self.remove_mqtt::<Messages>(MqttType::Messages, url)
                                }
                            },
                            ChronicleBrokerThrough::ExitProgram => {
                                supervisor.exit_program(false);
                            }
                        },
                        Err(other_app_event) => {
                            supervisor.passthrough(other_app_event, self.get_name());
                        }
                    },
                    BrokerEvent::Scylla(service) => {
                        if let Err(Need::Restart) = _status.as_ref() {
                            if service.is_running() {
                                // ask for restart, but first drop the handle to ensure we start with empty event_loop
                                self.handle.take();
                                return Err(Need::Restart);
                            }
                        }
                    }
                    BrokerEvent::Children(child) => {
                        let mut is_not_websocket_child = true;
                        match child {
                            BrokerChild::Listener(service) => {
                                self.service.update_microservice(service.get_name(), service.clone());
                            }
                            BrokerChild::Collector(service) => {
                                self.service.update_microservice(service.get_name(), service.clone());
                            }
                            BrokerChild::Solidifier(service, status) => {
                                // Handle abort
                                if let Err(Need::Abort) = status {
                                    if service.is_stopped() {
                                        // Pause broker app (is_stopping but awaitting on its event loop till scylla is
                                        // running)
                                        self.shutdown(supervisor, false).await;
                                        _status = Err(Need::Restart);
                                    }
                                }
                                self.service.update_microservice(service.get_name(), service.clone());
                            }
                            BrokerChild::Syncer(service, status) => {
                                // Handle abort
                                if let Err(Need::Abort) = status {
                                    if service.is_stopped() {
                                        // update status only if is not restarting
                                        if let Err(Need::Restart) = _status.as_ref() {
                                        } else {
                                            // Abort broker app
                                            self.shutdown(supervisor, true).await;
                                            _status = status;
                                        }
                                    }
                                }
                                self.service.update_microservice(service.get_name(), service.clone());
                            }
                            BrokerChild::Archiver(service, status) => {
                                // Handle abort
                                if let Err(Need::Abort) = status {
                                    if service.is_stopped() {
                                        // update status only if is not restarting
                                        if let Err(Need::Restart) = _status.as_ref() {
                                        } else {
                                            // Abort broker app
                                            self.shutdown(supervisor, true).await;
                                            _status = status;
                                        }
                                    }
                                }
                                self.service.update_microservice(service.get_name(), service.clone());
                            }
                            BrokerChild::Mqtt(service, mqtt_handle_opt, mqtt_status) => {
                                let microservice_name = service.get_name();
                                self.service.update_microservice(service.get_name(), service.clone());
                                match mqtt_status {
                                    Ok(()) => {
                                        if let Some(mqtt_handle) = mqtt_handle_opt {
                                            if !self.service.is_stopping() {
                                                self.mqtt_handles.insert(service.get_name(), mqtt_handle);
                                            } else {
                                                info!("Shutting down Mqtt: {}", service.get_name());
                                                mqtt_handle.shutdown();
                                            }
                                        }
                                    }
                                    Err(Need::Abort) => {
                                        // this is only possible while initializing with invalid/bad protocol
                                        // so we make sure to remove it from our service
                                        self.service.delete_microservice(&microservice_name);
                                    }
                                    Err(_) => {
                                        // this is Need::Restart
                                        // still we restart the service only if we didn't ask it to shutdown in first
                                        // place, as well the broker app is not
                                        // stopping and it should be stopped
                                        let asked_to_shutdown =
                                            self.asked_to_shutdown.get(&microservice_name).is_some();
                                        if !self.service.is_stopping() && service.is_stopped() && !asked_to_shutdown {
                                            // restart it by re-adding it, first we delete it
                                            self.service.delete_microservice(&microservice_name);
                                            // extract the url and topic from the name (topic@url)
                                            let mut name = microservice_name.split("@");
                                            let topic = name.next().unwrap();
                                            let url = Url::parse(name.next().unwrap()).unwrap();
                                            let restart_after = std::time::Duration::from_secs(5);
                                            warn!("Restarting Mqtt: {}, after: {:?}", microservice_name, restart_after);
                                            match Topics::try_from(topic).unwrap() {
                                                Topics::Messages => {
                                                    let new_mqtt =
                                                        self.add_mqtt(Messages, MqttType::Messages, url).unwrap();
                                                    tokio::spawn(
                                                        new_mqtt.start_after(restart_after, self.handle.clone()),
                                                    );
                                                }
                                                Topics::MessagesReferenced => {
                                                    let new_mqtt = self
                                                        .add_mqtt(MessagesReferenced, MqttType::MessagesReferenced, url)
                                                        .unwrap();
                                                    tokio::spawn(
                                                        new_mqtt.start_after(restart_after, self.handle.clone()),
                                                    );
                                                }
                                            }
                                        } else if asked_to_shutdown && service.is_stopped() {
                                            self.service.delete_microservice(&microservice_name);
                                            // remove it from asked_to_shutdown, only once the service.is_stopped
                                            self.asked_to_shutdown.remove(&microservice_name);
                                        }
                                    }
                                }
                            }
                            BrokerChild::Websocket(microservice, opt_ws_tx) => {
                                is_not_websocket_child = false;
                                if microservice.is_initializing() {
                                    // add ws_tx to websockets
                                    self.websockets.insert(microservice.name, opt_ws_tx.unwrap());
                                } else if microservice.is_stopped() {
                                    // remove it from websockets
                                    let mut ws_tx = self.websockets.remove(&microservice.name).unwrap();
                                    // make sure to close the websocket stream (optional)
                                    let _ = ws_tx.close().await;
                                }
                            }
                        }
                        if is_not_websocket_child {
                            // response to all websocket
                            let socket_msg = SocketMsg::ChronicleBroker(self.service.clone());
                            self.response_to_sockets(&socket_msg).await;
                            let SocketMsg::ChronicleBroker(service) = socket_msg;
                            // Inform launcher with status change
                            supervisor.status_change(service);
                        }
                    }
                }
            }
            _status
        } else {
            Err(Need::Abort)
        }
    }
}

impl<H: ChronicleBrokerScope> ChronicleBroker<H> {
    pub(crate) fn remove_mqtt<T: Topic>(&mut self, mqtt_type: MqttType, url: Url) {
        let microservice_name = format!("{}@{}", T::name(), url.as_str());
        if let Some(service) = self.service.microservices.get(&microservice_name) {
            // add it to asked_to_shutdown hashmap
            self.asked_to_shutdown.insert(microservice_name.clone(), ());
            if let Some(mqtt_handle) = self.mqtt_handles.remove(&microservice_name) {
                mqtt_handle.shutdown();
                let config = get_config();
                let mut new_config = config.clone();
                if let Some(list) = new_config.broker_config.mqtt_brokers.get_mut(&mqtt_type) {
                    list.remove(&url);
                }
                if new_config != config {
                    get_history_mut().update(new_config);
                }
            } else {
                // the mqtt maybe already in process to get shutdown, or we are trying to remove it before
                // it gets fully initialized
                warn!(
                    "No Mqtt handle: {}, service: {:?}",
                    microservice_name,
                    service.service_status()
                )
            };
        } else {
            // it doesn't exist
            error!(
                "The Mqtt: {}, you're trying to remove, it doesn't exist as service",
                microservice_name
            );
            // Maybe TODO response with something?;
        };
    }
    pub(crate) fn add_mqtt<T: Topic>(&mut self, topic: T, mqtt_type: MqttType, url: Url) -> Option<Mqtt<T>> {
        let mqtt = MqttBuilder::new()
            .collectors_handles(self.collector_handles.clone())
            .topic(topic)
            .url(url.clone())
            .build();
        let microservice = mqtt.clone_service();
        let microservice_name = microservice.get_name();
        if let None = self.service.microservices.get(&microservice_name) {
            self.service.update_microservice(microservice_name, microservice);
            let config = get_config();
            let mut new_config = config.clone();
            if let Some(list) = new_config.broker_config.mqtt_brokers.get_mut(&mqtt_type) {
                list.insert(url);
            }
            if new_config != config {
                get_history_mut().update(new_config);
            }
            Some(mqtt)
        } else {
            // it does already exist
            error!(
                "The Mqtt: {}, you're trying to add it does already exist as service",
                microservice_name
            );
            // TODO response with something;
            None
        }
    }
    pub(crate) async fn response_to_sockets<T: Serialize>(&mut self, msg: &SocketMsg<T>) {
        for socket in self.websockets.values_mut() {
            let j = serde_json::to_string(&msg).unwrap();
            let m = crate::websocket::Message::text(j);
            let _ = socket.send(m).await;
        }
    }
    pub(crate) async fn shutdown(&mut self, supervisor: &mut H, drop_handle: bool) {
        if !self.service.is_stopping() {
            // update service to be stopping, to prevent admins from changing the topology
            self.service.update_status(ServiceStatus::Stopping);
            // Ask launcher to shutdown broker application,
            // this is usefull in case the shutdown event sent by the websocket
            // client or being pass it from other application
            if drop_handle {
                supervisor.shutdown_app(&self.get_name());
            }
            // shutdown children
            // shutdown listener if provided
            if let Some(listener) = self.listener_handle.take() {
                listener.shutdown();
            }
            // shutdown mqtts
            for (mqtt_name, mqtt_handle) in self.mqtt_handles.drain() {
                info!("Shutting down Mqtt: {}", mqtt_name);
                mqtt_handle.shutdown();
            }
            // shutdown collectors
            for (collector_name, collector_handle) in self.collector_handles.drain() {
                info!("Shutting down Collector: {}", collector_name);
                collector_handle.shutdown();
            }
            // shutdown solidifiers
            for (solidifier_name, solidifier_handle) in self.solidifier_handles.drain() {
                info!("Shutting down Solidifier: {}", solidifier_name);
                solidifier_handle.shutdown();
            }
            // Shutdown the websockets
            for (_, ws) in &mut self.websockets {
                let _ = ws.close().await;
            }
            // shutdown syncer
            if let Some(syncer) = self.syncer_handle.take() {
                syncer.shutdown();
            }
            // drop self handler
            if drop_handle {
                self.handle.take();
            }
        }
    }
}
