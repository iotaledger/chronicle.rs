// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use anyhow::anyhow;

#[async_trait]
impl<H: WebsocketScope> Starter<H> for WebsocketBuilder<H> {
    type Ok = WebsocketSender<H>;

    type Error = anyhow::Error;

    type Input = Websocket<H>;

    async fn starter(self, handle: H, input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        let websocket = input.unwrap_or_else(|| self.build());

        let supervisor = websocket
            .sender
            .clone()
            .ok_or_else(|| anyhow!("No supervisor for websocket!"))?;

        tokio::spawn(websocket.start(Some(handle)));

        Ok(supervisor)
    }
}
