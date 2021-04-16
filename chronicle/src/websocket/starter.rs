// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use std::borrow::Cow;

#[async_trait]
impl<H: WebsocketScope> Starter<H> for WebsocketBuilder<H> {
    type Ok = WebsocketSender<H>;

    type Error = Cow<'static, str>;

    type Input = Websocket<H>;

    async fn starter(self, handle: H, input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        let websocket = self.build();

        let supervisor = websocket.sender.clone().unwrap();

        tokio::spawn(websocket.start(Some(handle)));

        Ok(supervisor)
    }
}
