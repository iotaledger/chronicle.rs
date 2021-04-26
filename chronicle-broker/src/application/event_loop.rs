// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::get_history_mut;
use futures::SinkExt;

#[async_trait]
impl<H: ChronicleBrokerScope> EventLoop<H> for ChronicleBroker<H> {
    async fn event_loop(&mut self, mut status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        status?;
        if let Some(ref mut supervisor) = supervisor {
            self.service.update_status(ServiceStatus::Running);
            while let Some(event) = self.inbox.recv().await {
                match event {
                    BrokerEvent::Importer(importer_session) => {
                        let socket_msg = super::SocketMsg::ChronicleBroker(importer_session);
                        self.response_to_sockets(&socket_msg).await;
                    }
                    BrokerEvent::Passthrough(passthrough_events) => match passthrough_events.try_get_my_event() {
                        Ok(my_event) => match my_event {
                            ChronicleBrokerThrough::Shutdown => {
                                self.shutdown(supervisor, true).await;
                                // ensure to drop handle
                                self.handle.take();
                            }
                            ChronicleBrokerThrough::Topology(topology) => {
                                if self.service.is_stopping() {
                                    // response that should not change the topology while is_stopping
                                    error!("Not supposed to dynamiclly change the topology while broker service is_stopped");
                                    let socket_msg = super::SocketMsg::ChronicleBroker(Err(topology));
                                    self.response_to_sockets::<Result<super::Topology, super::Topology>>(&socket_msg)
                                        .await;
                                    continue;
                                }
                                match topology {
                                    super::Topology::AddMqttMessages(url) => {
                                        if let Some(mqtt) = self.add_mqtt(Messages, MqttType::Messages, url) {
                                            tokio::spawn(mqtt.start(self.handle.clone()));
                                        }
                                    }
                                    super::Topology::AddMqttMessagesReferenced(url) => {
                                        if let Some(mqtt) =
                                            self.add_mqtt(MessagesReferenced, MqttType::MessagesReferenced, url)
                                        {
                                            tokio::spawn(mqtt.start(self.handle.clone()));
                                        }
                                    }
                                    super::Topology::RemoveMqttMessagesReferenced(url) => {
                                        self.remove_mqtt::<MessagesReferenced>(MqttType::MessagesReferenced, url)
                                    }
                                    super::Topology::RemoveMqttMessages(url) => {
                                        self.remove_mqtt::<Messages>(MqttType::Messages, url)
                                    }
                                    super::Topology::Import { .. } => {
                                        self.handle_import(topology).await;
                                        self.try_close_importer_session().await;
                                    }
                                    Topology::Requesters(requester_topology) => {
                                        match requester_topology {
                                            RequesterTopology::AddEndpoint(ref url) => {
                                                // todo verfiy the url
                                                self.collector_handles.values().for_each(|h| {
                                                    h.send_requester_topology(requester_topology.clone());
                                                });
                                            }
                                            RequesterTopology::RemoveEndpoint(_) => {
                                                self.collector_handles.values().for_each(|h| {
                                                    h.send_requester_topology(requester_topology.clone());
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                            ChronicleBrokerThrough::ExitProgram => {
                                supervisor.exit_program(false);
                            }
                        },
                        Err(other_app_event) => {
                            supervisor.passthrough(other_app_event, self.get_name());
                        }
                    },
                    BrokerEvent::Scylla(service) => {
                        if let Err(Need::Restart) = status.as_ref() {
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
                            BrokerChild::Importer(service, _status, parallelism) => {
                                if service.is_stopped() {
                                    self.in_progress_importers -= 1;
                                    self.service.delete_microservice(&service.get_name());
                                    // return parallelism
                                    self.parallelism_points += parallelism;
                                    // check if there are any pending
                                    if let Some(import_topology) = self.pending_imports.pop() {
                                        self.handle_import(import_topology).await;
                                    }
                                    // check if we finished all in_progress_importers
                                    self.try_close_importer_session().await;
                                } else {
                                    self.service.update_microservice(service.get_name(), service.clone());
                                }
                            }
                            BrokerChild::Solidifier(service, solidifier_status) => {
                                // Handle abort
                                if let Err(Need::Abort) = solidifier_status {
                                    if service.is_stopped() {
                                        // Pause broker app (is_stopping but awaitting on its event loop till scylla is
                                        // running)
                                        self.shutdown(supervisor, false).await;
                                        status = Err(Need::Restart);
                                    }
                                }
                                self.service.update_microservice(service.get_name(), service.clone());
                            }
                            BrokerChild::Syncer(service, syncer_status) => {
                                // Handle abort
                                if let Err(Need::Abort) = status {
                                    if service.is_stopped() {
                                        // update status only if is not restarting
                                        if let Err(Need::Restart) = status.as_ref() {
                                        } else {
                                            // Abort broker app
                                            self.shutdown(supervisor, true).await;
                                            status = syncer_status;
                                        }
                                    }
                                }
                                self.service.update_microservice(service.get_name(), service.clone());
                            }
                            BrokerChild::Archiver(service, archiver_status) => {
                                // Handle abort
                                if let Err(Need::Abort) = status {
                                    if service.is_stopped() {
                                        // update status only if is not restarting
                                        if let Err(Need::Restart) = status.as_ref() {
                                        } else {
                                            // Abort broker app
                                            self.shutdown(supervisor, true).await;
                                            status = archiver_status;
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
                            let socket_msg = super::SocketMsg::ChronicleBroker(self.service.clone());
                            self.response_to_sockets(&socket_msg).await;
                            let super::SocketMsg::ChronicleBroker(service) = socket_msg;
                            // Inform launcher with status change
                            supervisor.status_change(service);
                        }
                    }
                }
            }
            status
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
                    get_history_mut().update(new_config.into());
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
                get_history_mut().update(new_config.into());
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
    async fn handle_import(&mut self, import_topology: super::Topology) {
        if let super::Topology::Import {
            ref path,
            ref resume,
            ref import_range,
        } = import_topology
        {
            // don't do anything if the service is shutting down
            if self.service.is_stopping() {
                return ();
            }
            // check if we have enough parallelism points
            if self.parallelism_points == 0 {
                // add it to pending list
                self.pending_imports.push(import_topology);
                return ();
            }
            if path.is_file() {
                // build importer
                self.spawn_importer(path.clone(), *resume, import_range.clone(), self.parallelism_points)
                    .await;
            } else if path.is_dir() {
                self.spawn_importers(path.clone(), *resume, import_range.clone()).await;
            } else {
                let event = ImporterSession::PathError {
                    path: path.clone(),
                    msg: "Invalid path".into(),
                };
                let socket_msg = super::SocketMsg::ChronicleBroker(event);
                self.response_to_sockets(&socket_msg).await;
            }
        }
    }
    async fn try_close_importer_session(&mut self) {
        if self.in_progress_importers == 0 {
            let event = ImporterSession::Close;
            let socket_msg = super::SocketMsg::ChronicleBroker(event);
            self.response_to_sockets(&socket_msg).await;
        }
    }
    async fn spawn_importer(
        &mut self,
        file_path: PathBuf,
        resume: bool,
        import_range: Option<Range<u32>>,
        parallelism: u8,
    ) {
        // don't do anything if the service is shutting down
        if self.service.is_stopping() {
            return ();
        }
        if let Some(path_str) = file_path.to_str() {
            if self.service.microservices.get(&path_str.to_owned()).is_some() {
                return ();
            }
            let mut importer_builder = ImporterBuilder::new();
            if let Some(import_range) = import_range {
                importer_builder = importer_builder.import_range(import_range);
            };
            let importer = importer_builder
                .file_path(file_path)
                .resume(resume)
                .parallelism(parallelism)
                .retries_per_query(50) // TODO get it from config
                .chronicle_id(0) // TODO get it from config
                .build();
            let handle = importer.clone_handle().expect("Expected existing importer handle");
            self.importer_handles.insert(importer.get_name(), handle);
            let service = Service::new();
            self.service.update_microservice(importer.get_name(), service);
            tokio::spawn(importer.start(self.handle.clone()));
            self.in_progress_importers += 1;
            self.parallelism_points -= parallelism;
        } else {
            self.parallelism_points += parallelism;
            let event = ImporterSession::PathError {
                path: file_path,
                msg: "Unable to convert path to string".into(),
            };
            let socket_msg = super::SocketMsg::ChronicleBroker(event);
            self.response_to_sockets(&socket_msg).await;
        }
    }
    async fn spawn_importers(&mut self, path: PathBuf, resume: bool, import_range: Option<Range<u32>>) {
        let mut import_files = Vec::new();
        if let Ok(mut dir_entry) = tokio::fs::read_dir(&path).await {
            while let Ok(Some(p)) = dir_entry.next_entry().await {
                let file_path = p.path();
                if file_path.is_file() {
                    import_files.push(file_path);
                }
            }
        };
        let import_files_len = import_files.len();
        if import_files_len == 0 {
            let event = ImporterSession::PathError {
                path,
                msg: "No LogFiles in the provided path".into(),
            };
            let socket_msg = super::SocketMsg::ChronicleBroker(event);
            self.response_to_sockets(&socket_msg).await;
            return ();
        }
        if self.parallelism_points as usize > import_files_len {
            let parallelism = (self.parallelism_points as usize / import_files_len) as u8;
            for file_path in import_files {
                self.spawn_importer(file_path, resume, import_range.clone(), parallelism)
                    .await
            }
        } else {
            // unwrap is safe
            let file_path = import_files.pop().expect("Expected import file");
            self.spawn_importer(file_path, resume, import_range.clone(), self.parallelism_points)
                .await;
            // convert any remaining into pending_imports
            for file_path in import_files {
                let topology = super::Topology::Import {
                    path: file_path,
                    resume,
                    import_range: import_range.clone(),
                };
                self.pending_imports.push(topology);
            }
        }
    }
    pub(crate) async fn response_to_sockets<T: Serialize>(&mut self, msg: &super::SocketMsg<T>) {
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
            // shutdown importers
            for (importer_name, importer_handle) in self.importer_handles.drain() {
                info!("Shutting down importer: {}", importer_name);
                importer_handle.shutdown();
            }
            // drop self handler
            if drop_handle {
                self.handle.take();
            }
        }
    }
}
