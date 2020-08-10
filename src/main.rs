//! Tool to vet mqtt brokers. The goal here is to test and benchmark
//! mqtt brokers for features, robustness, performance and scalability
//!
//! Goals of the test suite
//!
//! - Spawn n clients with publish and subscribe on the same topic (and report thoughput and latencies)
//! - Spawn n clinets with publishes and 1 subscription to pull all the data (used to simulate a sink in the cloud)
//! - Offline messaging
//! - Halfopen connection detection
//!
#[macro_use]
extern crate log;

use futures;
use argh::FromArgs;
use tokio::task;
use std::sync::Arc;

mod connection;

#[derive(FromArgs)]
/// Reach new heights.
struct Config {
    /// number of connections
    #[argh(option, short = 'c', default = "1")]
    connections: usize,

    /// size of payload
    #[argh(option, short = 'p', default = "1024")]
    payload_size: usize,

    /// number of messages
    #[argh(option, short = 'n', default = "10000")]
    count: u16,

    /// server
    #[argh(option, short = 's', default = "String::from(\"localhost\")")]
    server: String,

    /// port
    #[argh(option, short = 'P', default = "8883")]
    port: u16,

    /// keep alive
    #[argh(option, short = 'k', default = "10")]
    keep_alive: u16,

    /// max inflight messages
    #[argh(option, short = 'q', default = "200")]
    max_inflight: u16,

    /// path to PEM encoded x509 ca-chain file
    #[argh(option, short = 'R')]
    ca_file: Option<String>,

    /// path to PEM encoded x509 client cert file.
    #[argh(option, short = 'C')]
    client_cert: Option<String>,

    /// path to PEM encoded client key file
    #[argh(option, short = 'K')]
    client_key: Option<String>,

    /// connection_timeout
    #[argh(option, short = 'T', default = "5")]
    conn_timeout: u64,

    /// qos, default 1
    #[argh(option, short = 'Q', default = "1")]
    qos: i16,

    /// number of publishers, default 1
    #[argh(option, short = 'n', default = "1")]
    publishers: i16,

    /// number of subscribers, default 1
    #[argh(option, short = 'm', default = "1")]
    subscribers: i16,
}

#[derive(Debug, Clone)]
pub struct Metrics {
    progress: u16,
}

#[tokio::main(core_threads = 4)]
async fn main() {
    pretty_env_logger::init();
    let config: Config = argh::from_env();
    let mut handles = vec![];

    let connections = config.connections;
    let config = Arc::new(config);
    for i in 0..connections {
        let config = config.clone();
        handles.push(task::spawn(async move {
            let id = format!("mqtt-{}", i);
            let mut connection = connection::Connection::new(id, config) ;
            connection.start().await;
        }));
    }

    futures::future::join_all(handles).await;
}
