// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<T: Topic, H: PermanodeBrokerScope> Init<BrokerHandle<H>> for Mqtt<T> {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        // create async client
        let create_opts = CreateOptionsBuilder::new()
            .server_uri(&self.url.as_str()[..])
            .client_id(&self.get_name()[..])
            .persistence(None)
            .finalize();
        let mut client = AsyncClient::new(create_opts).map_err(|e| {
            error!("Unable to create AsyncClient: {}, error: {}", &self.url.as_str(), e);
            Need::Abort
        })?;
        info!("Created AsyncClient: {}", &self.url.to_string());
        let stream = client.get_stream(self.stream_capacity);
        let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .mqtt_version(paho_mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .connect_timeout(Duration::from_secs(10))
            .finalize();
        // connect client with the remote broker
        client.connect(conn_opts).await.map_err(|e| {
            error!(
                "Unable to connect AsyncClient: {}, topic: {}, error: {}",
                &self.url.as_str(),
                T::name(),
                e
            );
            Need::Restart
        })?;
        info!("Connected AsyncClient: {}", &self.url.as_str());
        // subscribe to T::name() topic with T::qos()
        client.subscribe(T::name(), T::qos()).await.map_err(|e| {
            error!(
                "Unable to subscribe AsyncClient: {}, topic: {}, error: {}",
                &self.url.as_str(),
                T::name(),
                e
            );
            Need::Restart
        })?;
        info!("Subscribed AsyncClient: {}, topic: {}", &self.url.as_str(), T::name());
        let event = BrokerEvent::Children(BrokerChild::Mqtt(
            self.service.clone(),
            Some(MqttHandle { client }),
            Ok(()),
        ));
        let _ = supervisor.as_mut().unwrap().send(event);
        // create inbox
        self.inbox.replace(MqttInbox { stream });
        status
    }
}
