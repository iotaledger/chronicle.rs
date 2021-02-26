// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<T: Topic, H: PermanodeBrokerScope> Init<BrokerHandle<H>> for Mqtt<T> {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        // create async client
        let mut cli =
            AsyncClient::new((&self.address.to_string()[..], &self.get_name()[..])).map_err(|_| Need::Abort)?;
        let stream = cli.get_stream(self.stream_capacity);
        let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .mqtt_version(paho_mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .connect_timeout(Duration::from_secs(1))
            .finalize();
        // connect client with the remote broker
        cli.connect(conn_opts).await.map_err(|_| Need::Restart)?;
        // subscribe to T::name() topic with T::qos()
        cli.subscribe(T::name(), T::qos()).await.map_err(|_| Need::Restart)?;
        // TODO send client/handle to supervisor

        let event = BrokerEvent::Children(BrokerChild::Mqtt(self.service.clone()));
        let _ = supervisor.as_mut().unwrap().send(event);
        // create inbox
        self.inbox = Some(MqttInbox {
            stream: Box::new(stream),
        });
        status
    }
}
