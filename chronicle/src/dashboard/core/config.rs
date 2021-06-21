// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use serde::Deserialize;

const DEFAULT_SESSION_TIMEOUT: u64 = 86400;
const DEFAULT_USER: &str = "admin";
// TODO: assign the password by from the config file
// Current user:password = admin:test
const DEFAULT_PASSWORD_SALT: &str = "9c970dd7e82cd7d76f412af06e9eb3e493c7882165d2eb43848bf5355d16b286";
const DEFAULT_PASSWORD_HASH: &str = "a84bd5bbda37c0e9f31054c9bf7155e357f8f26929e6f0fc7ff0068965c127b8";
const DEFAULT_PORT: u16 = 8081;

#[derive(Default, Deserialize)]
pub struct DashboardAuthConfigBuilder {
    session_timeout: Option<u64>,
    user: Option<String>,
    password_salt: Option<String>,
    password_hash: Option<String>,
}

impl DashboardAuthConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finish(self) -> DashboardAuthConfig {
        DashboardAuthConfig {
            session_timeout: self.session_timeout.unwrap_or(DEFAULT_SESSION_TIMEOUT),
            user: self.user.unwrap_or_else(|| DEFAULT_USER.to_owned()),
            password_salt: self.password_salt.unwrap_or_else(|| DEFAULT_PASSWORD_SALT.to_owned()),
            password_hash: self.password_hash.unwrap_or_else(|| DEFAULT_PASSWORD_HASH.to_owned()),
        }
    }
}

#[derive(Clone)]
pub struct DashboardAuthConfig {
    session_timeout: u64,
    user: String,
    password_salt: String,
    password_hash: String,
}

// TODO: Build the DashboardConfig from config file
#[allow(dead_code)]
impl DashboardAuthConfig {
    pub fn build() -> DashboardAuthConfigBuilder {
        DashboardAuthConfigBuilder::new()
    }

    pub fn session_timeout(&self) -> u64 {
        self.session_timeout
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn password_salt(&self) -> &str {
        &self.password_salt
    }

    pub fn password_hash(&self) -> &str {
        &self.password_hash
    }
}

// TODO: Build the DashboardConfig from config file
#[allow(dead_code)]
#[derive(Default, Deserialize)]
pub struct DashboardConfigBuilder {
    port: Option<u16>,
    auth: Option<DashboardAuthConfigBuilder>,
}

// TODO: Build the DashboardConfig from config file
#[allow(dead_code)]
impl DashboardConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finish(self) -> DashboardConfig {
        DashboardConfig {
            port: self.port.unwrap_or(DEFAULT_PORT),
            auth: self.auth.unwrap_or_default().finish(),
        }
    }
}

#[derive(Clone)]
pub struct DashboardConfig {
    port: u16,
    auth: DashboardAuthConfig,
}

// TODO: Build the DashboardConfig from config file
#[allow(dead_code)]
impl DashboardConfig {
    pub fn build() -> DashboardConfigBuilder {
        DashboardConfigBuilder::new()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn auth(&self) -> &DashboardAuthConfig {
        &self.auth
    }
}
