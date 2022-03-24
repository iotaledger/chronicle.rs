// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use cfg_if::cfg_if;
use mongodb::{
    bson::Document,
    options::{
        ClientOptions,
        ReadConcern,
        ReadPreferenceOptions,
        ServerApi,
        WriteConcern,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    path::PathBuf,
    time::Duration,
};

/// A clone of the `ClientOptions` structure from the `mongodb` crate
/// which can be Serialized.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MongoConfig {
    /// The initial list of seeds that the Client should connect to.
    ///
    /// Note that by default, the driver will autodiscover other nodes in the cluster. To connect
    /// directly to a single server (rather than autodiscovering the rest of the cluster), set the
    /// `direct_connection` field to `true`.
    #[serde(default = "default_hosts")]
    pub hosts: Vec<ServerAddress>,

    /// The application name that the Client will send to the server as part of the handshake. This
    /// can be used in combination with the server logs to determine which Client is connected to a
    /// server.
    pub app_name: Option<String>,

    /// The compressors that the Client is willing to use in the order they are specified
    /// in the configuration.  The Client sends this list of compressors to the server.
    /// The server responds with the intersection of its supported list of compressors.
    /// The order of compressors indicates preference of compressors.
    pub compressors: Option<Vec<Compressor>>,

    /// The connect timeout passed to each underlying TcpStream when attempting to connect to the
    /// server.
    ///
    /// The default value is 10 seconds.
    pub connect_timeout: Option<Duration>,

    /// The credential to use for authenticating connections made by this client.
    pub credential: Option<Credential>,

    /// Specifies whether the Client should directly connect to a single host rather than
    /// autodiscover all servers in the cluster.
    ///
    /// The default value is false.
    pub direct_connection: Option<bool>,

    /// Extra information to append to the driver version in the metadata of the handshake with the
    /// server. This should be used by libraries wrapping the driver, e.g. ODMs.
    pub driver_info: Option<DriverInfo>,

    /// The amount of time each monitoring thread should wait between sending an isMaster command
    /// to its respective server.
    ///
    /// The default value is 10 seconds.
    pub heartbeat_freq: Option<Duration>,

    /// When running a read operation with a ReadPreference that allows selecting secondaries,
    /// `local_threshold` is used to determine how much longer the average round trip time between
    /// the driver and server is allowed compared to the least round trip time of all the suitable
    /// servers. For example, if the average round trip times of the suitable servers are 5 ms, 10
    /// ms, and 15 ms, and the local threshold is 8 ms, then the first two servers are within the
    /// latency window and could be chosen for the operation, but the last one is not.
    ///
    /// A value of zero indicates that there is no latency window, so only the server with the
    /// lowest average round trip time is eligible.
    ///
    /// The default value is 15 ms.
    pub local_threshold: Option<Duration>,

    /// The amount of time that a connection can remain idle in a connection pool before being
    /// closed. A value of zero indicates that connections should not be closed due to being idle.
    ///
    /// By default, connections will not be closed due to being idle.
    pub max_idle_time: Option<Duration>,

    /// The maximum amount of connections that the Client should allow to be created in a
    /// connection pool for a given server. If an operation is attempted on a server while
    /// `max_pool_size` connections are checked out, the operation will block until an in-progress
    /// operation finishes and its connection is checked back into the pool.
    ///
    /// The default value is 100.
    pub max_pool_size: Option<u32>,

    /// The minimum number of connections that should be available in a server's connection pool at
    /// a given time. If fewer than `min_pool_size` connections are in the pool, connections will
    /// be added to the pool in the background until `min_pool_size` is reached.
    ///
    /// The default value is 0.
    pub min_pool_size: Option<u32>,

    /// Specifies the default read concern for operations performed on the Client. See the
    /// ReadConcern type documentation for more details.
    pub read_concern: Option<ReadConcern>,

    /// The name of the replica set that the Client should connect to.
    pub repl_set_name: Option<String>,

    /// Whether or not the client should retry a read operation if the operation fails.
    ///
    /// The default value is true.
    pub retry_reads: Option<bool>,

    /// Whether or not the client should retry a write operation if the operation fails.
    ///
    /// The default value is true.
    pub retry_writes: Option<bool>,

    /// The default selection criteria for operations performed on the Client. See the
    /// SelectionCriteria type documentation for more details.
    pub selection_criteria: Option<ReadPreference>,

    /// The declared API version for this client.
    /// The declared API version is applied to all commands run through the client, including those
    /// sent through any handle derived from the client.
    ///
    /// Specifying versioned API options in the command document passed to `run_command` AND
    /// declaring an API version on the client is not supported and is considered undefined
    /// behaviour. To run any command with a different API version or without declaring one, create
    /// a separate client that declares the appropriate API version.
    ///
    /// For more information, see the [Versioned API](
    /// https://docs.mongodb.com/v5.0/reference/versioned-api/) manual page.
    pub server_api: Option<ServerApi>,

    /// The amount of time the Client should attempt to select a server for an operation before
    /// timing outs
    ///
    /// The default value is 30 seconds.
    pub server_selection_timeout: Option<Duration>,

    /// Default database for this client.
    ///
    /// By default, no default database is specified.
    pub default_database: Option<String>,

    /// The TLS configuration for the Client to use in its connections with the server.
    ///
    /// By default, TLS is disabled.
    pub tls: Option<Tls>,

    /// Specifies the default write concern for operations performed on the Client. See the
    /// WriteConcern type documentation for more details.
    pub write_concern: Option<WriteConcern>,
}

impl Default for MongoConfig {
    fn default() -> Self {
        Self {
            hosts: default_hosts(),
            app_name: Default::default(),
            compressors: Default::default(),
            connect_timeout: Default::default(),
            credential: Default::default(),
            direct_connection: Default::default(),
            driver_info: Default::default(),
            heartbeat_freq: Default::default(),
            local_threshold: Default::default(),
            max_idle_time: Default::default(),
            max_pool_size: Default::default(),
            min_pool_size: Default::default(),
            read_concern: Default::default(),
            repl_set_name: Default::default(),
            retry_reads: Default::default(),
            retry_writes: Default::default(),
            selection_criteria: Default::default(),
            server_api: Default::default(),
            server_selection_timeout: Default::default(),
            default_database: Default::default(),
            tls: Default::default(),
            write_concern: Default::default(),
        }
    }
}

impl Into<ClientOptions> for MongoConfig {
    fn into(self) -> ClientOptions {
        let builder = ClientOptions::builder()
            .hosts(self.hosts.into_iter().map(Into::into).collect::<Vec<_>>())
            .app_name(self.app_name)
            .compressors(self.compressors.map(|v| v.into_iter().map(Into::into).collect()))
            .connect_timeout(self.connect_timeout)
            .credential(self.credential.map(Into::into))
            .direct_connection(self.direct_connection)
            .driver_info(self.driver_info.map(Into::into))
            .heartbeat_freq(self.heartbeat_freq)
            .local_threshold(self.local_threshold)
            .max_idle_time(self.max_idle_time)
            .max_pool_size(self.max_pool_size)
            .min_pool_size(self.min_pool_size)
            .read_concern(self.read_concern)
            .repl_set_name(self.repl_set_name)
            .retry_reads(self.retry_reads)
            .retry_writes(self.retry_writes)
            .selection_criteria(self.selection_criteria.map(Into::into))
            .server_api(self.server_api)
            .server_selection_timeout(self.server_selection_timeout)
            .default_database(self.default_database)
            .tls(self.tls.map(Into::into))
            .write_concern(self.write_concern);
        builder.build()
    }
}

/// An enum representing the address of a MongoDB server.
///
/// Currently this just supports addresses that can be connected to over TCP, but alternative
/// address types may be supported in the future (e.g. Unix Domain Socket paths).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServerAddress {
    /// A TCP/IP host and port combination.
    Tcp {
        /// The hostname or IP address where the MongoDB server can be found.
        host: String,

        /// The TCP port that the MongoDB server is listening on.
        ///
        /// The default is 27017.
        port: Option<u16>,
    },
}

impl Default for ServerAddress {
    fn default() -> Self {
        Self::Tcp {
            host: "localhost".to_string(),
            port: Some(27017),
        }
    }
}

impl Into<mongodb::options::ServerAddress> for ServerAddress {
    fn into(self) -> mongodb::options::ServerAddress {
        match self {
            ServerAddress::Tcp { host, port } => mongodb::options::ServerAddress::Tcp { host, port },
        }
    }
}

fn default_hosts() -> Vec<ServerAddress> {
    vec![ServerAddress::default()]
}

/// Enum representing supported compressor algorithms.
/// Used for compressing and decompressing messages sent to and read from the server.
/// For compressors that take a `level`, use `None` to indicate the default level.
/// Higher `level` indicates more compression (and slower).
/// Requires `zstd-compression` feature flag to use `Zstd` compressor,
/// `zlib-compression` feature flag to use `Zlib` compressor, and
/// `snappy-compression` feature flag to use `Snappy` Compressor.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Compressor {
    /// Zstd compressor.  Requires Rust version 1.54.
    /// See [`Zstd`](http://facebook.github.io/zstd/zstd_manual.html) for more information
    Zstd {
        /// Zstd compression level
        level: Option<i32>,
    },
    /// Zlib compressor.
    /// See [`Zlib`](https://zlib.net/) for more information.
    Zlib {
        /// Zlib compression level
        level: Option<i32>,
    },
    /// Snappy compressor.
    /// See [`Snappy`](http://google.github.io/snappy/) for more information.
    Snappy,
}

impl Into<mongodb::options::Compressor> for Compressor {
    fn into(self) -> mongodb::options::Compressor {
        match self {
            Compressor::Zstd {
                #[cfg(feature = "mongodb/zstd-compression")]
                level,
                #[cfg(not(feature = "mongodb/zstd-compression"))]
                    level: _,
            } => {
                cfg_if! {
                    if #[cfg(feature = "mongodb/zstd-compression")] {
                        mongodb::options::Compressor::Zstd { level }
                    } else {
                        panic!("mongodb/zstd-compression feature flag not enabled")
                    }
                }
            }
            Compressor::Zlib {
                #[cfg(feature = "mongodb/zlib-compression")]
                level,
                #[cfg(not(feature = "mongodb/zlib-compression"))]
                    level: _,
            } => {
                cfg_if! {
                    if #[cfg(feature = "mongodb/zlib-compression")] {
                        mongodb::options::Compressor::Zlib { level }
                    } else {
                        panic!("mongodb/zlib-compression feature flag not enabled")
                    }
                }
            }
            Compressor::Snappy => {
                cfg_if! {
                    if #[cfg(feature = "mongodb/snappy-compression")] {
                        mongodb::options::Compressor::Snappy
                    } else {
                        panic!("mongodb/snappy-compression feature flag not enabled")
                    }
                }
            }
        }
    }
}

/// Specifies whether TLS configuration should be used with the operations that the
/// [`Client`](../struct.Client.html) performs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Tls {
    /// Enable TLS with the specified options.
    Enabled(TlsOptions),

    /// Disable TLS.
    Disabled,
}

impl Into<mongodb::options::Tls> for Tls {
    fn into(self) -> mongodb::options::Tls {
        match self {
            Tls::Enabled(tls_options) => mongodb::options::Tls::Enabled(tls_options.into()),
            Tls::Disabled => mongodb::options::Tls::Disabled,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[allow(missing_docs)]
pub struct TlsOptions {
    /// Whether or not the [`Client`](../struct.Client.html) should return an error if the server
    /// presents an invalid certificate. This setting should _not_ be set to `true` in
    /// production; it should only be used for testing.
    ///
    /// The default value is to error when the server presents an invalid certificate.
    pub allow_invalid_certificates: Option<bool>,

    /// The path to the CA file that the [`Client`](../struct.Client.html) should use for TLS. If
    /// none is specified, then the driver will use the Mozilla root certificates from the
    /// `webpki-roots` crate.
    pub ca_file_path: Option<PathBuf>,

    /// The path to the certificate file that the [`Client`](../struct.Client.html) should present
    /// to the server to verify its identify. If none is specified, then the
    /// [`Client`](../struct.Client.html) will not attempt to verify its identity to the
    /// server.
    pub cert_key_file_path: Option<PathBuf>,
}

impl Into<mongodb::options::TlsOptions> for TlsOptions {
    fn into(self) -> mongodb::options::TlsOptions {
        mongodb::options::TlsOptions::builder()
            .allow_invalid_certificates(self.allow_invalid_certificates)
            .ca_file_path(self.ca_file_path)
            .cert_key_file_path(self.cert_key_file_path)
            .build()
    }
}

/// Specifies how the driver should route a read operation to members of a replica set.
///
/// If applicable, `tag_sets` can be used to target specific nodes in a replica set, and
/// `max_staleness` specifies the maximum lag behind the primary that a secondary can be to remain
/// eligible for the operation. The max staleness value maps to the `maxStalenessSeconds` MongoDB
/// option and will be sent to the server as an integer number of seconds.
///
/// See the [MongoDB docs](https://docs.mongodb.com/manual/core/read-preference) for more details.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum ReadPreference {
    /// Only route this operation to the primary.
    Primary,

    /// Only route this operation to a secondary.
    Secondary { options: ReadPreferenceOptions },

    /// Route this operation to the primary if it's available, but fall back to the secondaries if
    /// not.
    PrimaryPreferred { options: ReadPreferenceOptions },

    /// Route this operation to a secondary if one is available, but fall back to the primary if
    /// not.
    SecondaryPreferred { options: ReadPreferenceOptions },

    /// Route this operation to the node with the least network latency regardless of whether it's
    /// the primary or a secondary.
    Nearest { options: ReadPreferenceOptions },
}

impl Into<mongodb::options::SelectionCriteria> for ReadPreference {
    fn into(self) -> mongodb::options::SelectionCriteria {
        mongodb::options::SelectionCriteria::ReadPreference(self.into())
    }
}

impl Into<mongodb::options::ReadPreference> for ReadPreference {
    fn into(self) -> mongodb::options::ReadPreference {
        match self {
            ReadPreference::Primary => mongodb::options::ReadPreference::Primary,
            ReadPreference::Secondary { options } => mongodb::options::ReadPreference::Secondary { options },
            ReadPreference::PrimaryPreferred { options } => {
                mongodb::options::ReadPreference::PrimaryPreferred { options }
            }
            ReadPreference::SecondaryPreferred { options } => {
                mongodb::options::ReadPreference::SecondaryPreferred { options }
            }
            ReadPreference::Nearest { options } => mongodb::options::ReadPreference::Nearest { options },
        }
    }
}

/// Extra information to append to the driver version in the metadata of the handshake with the
/// server. This should be used by libraries wrapping the driver, e.g. ODMs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DriverInfo {
    /// The name of the library wrapping the driver.
    pub name: String,

    /// The version of the library wrapping the driver.
    pub version: Option<String>,

    /// Optional platform information for the wrapping driver.
    pub platform: Option<String>,
}

impl Into<mongodb::options::DriverInfo> for DriverInfo {
    fn into(self) -> mongodb::options::DriverInfo {
        mongodb::options::DriverInfo::builder()
            .name(self.name)
            .version(self.version)
            .platform(self.platform)
            .build()
    }
}

/// A struct containing authentication information.
///
/// Some fields (mechanism and source) may be omitted and will either be negotiated or assigned a
/// default value, depending on the values of other fields in the credential.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Credential {
    /// The username to authenticate with. This applies to all mechanisms but may be omitted when
    /// authenticating via MONGODB-X509.
    pub username: Option<String>,

    /// The database used to authenticate. This applies to all mechanisms and defaults to "admin"
    /// in SCRAM authentication mechanisms, "$external" for GSSAPI and MONGODB-X509, and the
    /// database name or "$external" for PLAIN.
    pub source: Option<String>,

    /// The password to authenticate with. This does not apply to all mechanisms.
    pub password: Option<String>,

    /// Which authentication mechanism to use. If not provided, one will be negotiated with the
    /// server.
    pub mechanism: Option<AuthMechanism>,

    /// Additional properties for the given mechanism.
    pub mechanism_properties: Option<Document>,
}

impl Into<mongodb::options::Credential> for Credential {
    fn into(self) -> mongodb::options::Credential {
        mongodb::options::Credential::builder()
            .username(self.username)
            .source(self.source)
            .password(self.password)
            .mechanism(self.mechanism.map(Into::into))
            .mechanism_properties(self.mechanism_properties)
            .build()
    }
}

/// The authentication mechanisms supported by MongoDB.
///
/// Note: not all of these mechanisms are currently supported by the driver.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum AuthMechanism {
    /// MongoDB Challenge Response nonce and MD5 based authentication system. It is currently
    /// deprecated and will never be supported by this driver.
    MongoDbCr,

    /// The SCRAM-SHA-1 mechanism as defined in [RFC 5802](http://tools.ietf.org/html/rfc5802).
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-scram/) for more information.
    ScramSha1,

    /// The SCRAM-SHA-256 mechanism which extends [RFC 5802](http://tools.ietf.org/html/rfc5802) and is formally defined in [RFC 7677](https://tools.ietf.org/html/rfc7677).
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-scram/) for more information.
    ScramSha256,

    /// The MONGODB-X509 mechanism based on the usage of X.509 certificates to validate a client
    /// where the distinguished subject name of the client certificate acts as the username.
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-x.509/) for more information.
    MongoDbX509,

    /// Kerberos authentication mechanism as defined in [RFC 4752](http://tools.ietf.org/html/rfc4752).
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/kerberos/) for more information.
    ///
    /// Note: This mechanism is not currently supported by this driver but will be in the future.
    Gssapi,

    /// The SASL PLAIN mechanism, as defined in [RFC 4616](), is used in MongoDB to perform LDAP
    /// authentication and cannot be used for any other type of authentication.
    /// Since the credentials are stored outside of MongoDB, the "$external" database must be used
    /// for authentication.
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-ldap/#ldap-proxy-authentication) for more information on LDAP authentication.
    Plain,

    /// MONGODB-AWS authenticates using AWS IAM credentials (an access key ID and a secret access
    /// key), temporary AWS IAM credentials obtained from an AWS Security Token Service (STS)
    /// Assume Role request, or temporary AWS IAM credentials assigned to an EC2 instance or ECS
    /// task.
    ///
    /// Note: Only server versions 4.4+ support AWS authentication. Additionally, the driver only
    /// supports AWS authentication with the tokio runtime.
    MongoDbAws,
}

impl Into<mongodb::options::AuthMechanism> for AuthMechanism {
    fn into(self) -> mongodb::options::AuthMechanism {
        match self {
            AuthMechanism::MongoDbCr => mongodb::options::AuthMechanism::MongoDbCr,
            AuthMechanism::ScramSha1 => mongodb::options::AuthMechanism::ScramSha1,
            AuthMechanism::ScramSha256 => mongodb::options::AuthMechanism::ScramSha256,
            AuthMechanism::MongoDbX509 => mongodb::options::AuthMechanism::MongoDbX509,
            AuthMechanism::Gssapi => mongodb::options::AuthMechanism::Gssapi,
            AuthMechanism::Plain => mongodb::options::AuthMechanism::Plain,
            AuthMechanism::MongoDbAws => {
                cfg_if! {
                    if #[cfg(feature = "mongodb/aws-auth")] {
                        mongodb::options::AuthMechanism::MongoDbAws
                    } else {
                        panic!("mongodb/aws-auth feature flag not enabled")
                    }
                }
            }
        }
    }
}
