// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! This module implements the response part of the challenge–response authentication.

use super::{encoder::BE_0_BYTES_LEN, header::Header, opcode::AUTH_RESPONSE};
use crate::compression::Compression;
use std::convert::TryInto;

/// The Authenticator structure with the token field.
pub trait Authenticator: Clone {
    /// Get the token in the Authenticator.
    fn token(&self) -> Vec<u8>;
}
#[derive(Clone)]
/// The unit structure used for letting all users be autenticated.
pub struct AllowAllAuth;

impl Authenticator for AllowAllAuth {
    // Return token as [bytes]
    fn token(&self) -> Vec<u8> {
        // [int] n, followed by n-bytes
        vec![0, 0, 0, 1, 0]
    }
}

#[derive(Clone)]
/// The password autentication structure with the user and password fields.
pub struct PasswordAuth {
    user: String,
    pass: String,
}

impl PasswordAuth {
    /// Create a new user with account and the corresponding password.
    pub fn new(user: String, pass: String) -> Self {
        Self { user, pass }
    }
}

impl Authenticator for PasswordAuth {
    fn token(&self) -> Vec<u8> {
        // compute length in advance
        let length = self.user.len() + self.pass.len() + 2;
        let mut token = Vec::new();
        token.extend_from_slice(&i32::to_be_bytes(length.try_into().unwrap()));
        token.push(0);
        token.extend_from_slice(self.user.as_bytes());
        token.push(0);
        token.extend_from_slice(self.pass.as_bytes());
        token
    }
}

/// The autentication response frame.
pub struct AuthResponse(pub Vec<u8>);

impl Header for AuthResponse {
    fn new() -> Self {
        AuthResponse(Vec::new())
    }
    fn with_capacity(capacity: usize) -> Self {
        AuthResponse(Vec::with_capacity(capacity))
    }
    fn version(mut self) -> Self {
        self.0.push(4);
        self
    }
    fn flags(mut self, flags: u8) -> Self {
        self.0.push(flags);
        self
    }
    fn stream(mut self, stream: i16) -> Self {
        self.0.extend(&i16::to_be_bytes(stream));
        self
    }
    fn opcode(mut self) -> Self {
        self.0.push(AUTH_RESPONSE);
        self
    }
    fn length(mut self) -> Self {
        self.0.extend(&BE_0_BYTES_LEN);
        self
    }
}

impl AuthResponse {
    /// Update the response token to be the token from autenticator.
    pub fn token(mut self, authenticator: &impl Authenticator) -> Self {
        let token = authenticator.token();
        self.0.extend(token);
        self
    }
    /// Build a response frame with a assigned compression type.
    pub fn build(mut self, compression: impl Compression) -> Self {
        self.0 = compression.compress(self.0);
        self
    }
}
