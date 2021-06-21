// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::dashboard::{
    core::{
        config::DashboardAuthConfigBuilder,
        routes,
    },
    websocket::{
        WsUsers,
        WsWorker,
        WsWorkers,
    },
    workers::node_status::node_status_worker,
};
use bee_runtime::shutdown_stream::ShutdownStream;
use futures::channel::oneshot;
use log::info;
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;

use std::{
    net::{
        IpAddr,
        Ipv4Addr,
        SocketAddr,
    },
    time::Duration,
};

#[async_trait]
impl<H: DashboardScope> EventLoop<H> for Dashboard<H> {
    async fn event_loop(&mut self, _status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        if let Some(ref mut supervisor) = supervisor {
            supervisor.status_change(self.service.clone());

            // The worker registers
            let workers = WsWorkers::default();

            // The user registers
            let users = WsUsers::default();
            // TODO: get the address from the config
            let websocket_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8081);

            // TODO: Currently we need to send fake data to the frontend to enable the dashboard
            // TODO: Register the workers so as to shutdown them gracefully
            let (worker_shutdown_tx, worker_shutdown_rx) = oneshot::channel();
            let ticker = ShutdownStream::new(
                worker_shutdown_rx,
                IntervalStream::new(interval(Duration::from_secs(1))),
            );
            node_status_worker(ticker, &users);

            // Insert the worker
            let worker_name = "node_status_worker".to_string();
            workers.write().await.insert(
                worker_name.clone(),
                WsWorker {
                    shutdown: Some(worker_shutdown_tx),
                },
            );

            let routes = routes::routes(
                "my_permanode_id".to_string(),
                DashboardAuthConfigBuilder::new().finish(),
                users.clone(),
            );

            info!("Dashboard available at {}.", websocket_address);

            let handle = tokio::spawn(warp::serve(routes).run(websocket_address));

            while let Some(evt) = self.inbox.recv().await {
                match evt {
                    DashboardEvent::Passthrough(passthrough_events) => match passthrough_events.try_get_my_event() {
                        Ok(my_event) => match my_event {
                            DashboardThrough::Shutdown => {
                                if !self.service.is_stopping() {
                                    supervisor.shutdown_app(&self.get_name());
                                    handle.abort();
                                    self.sender.take();

                                    // Shutdown all users and workers
                                    let mut readies = Vec::new();

                                    for (_, user) in users.write().await.iter_mut() {
                                        if let Some(shutdown) = user.shutdown.take() {
                                            let _ = shutdown.send(());
                                            readies.push(user.shutdown_ready.take().unwrap());
                                        }
                                    }

                                    futures::future::join_all(readies).await;

                                    for (_, worker) in workers.write().await.iter_mut() {
                                        if let Some(shutdown) = worker.shutdown.take() {
                                            let _ = shutdown.send(());
                                        }
                                    }

                                    info!("All users stopped.");

                                    // TODO: Need to register workers in a global structure
                                    // let _ = worker_shutdown_tx.send(());
                                    // info!("Worker stopped.")
                                }
                            }
                        },
                        Err(other_app_event) => {
                            supervisor.passthrough(other_app_event, self.get_name());
                        }
                    },
                }
            }
        }

        Ok(())
    }
}
