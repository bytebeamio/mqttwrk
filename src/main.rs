//! Tool to vet mqtt brokers. The goal here is to test and benchmark
//! mqtt brokers for features, robustness, performance and scalability
//!
//! Goals of the test suite
//!
//! - Spawn n clients with publish and subscribe on the same topic (and report thoughput and latencies)
//! - Spawn n clinets with publishes and 1 subscription to pull all the data (used to simulate a sink in the cloud)
//! - Offline messaging
//! - Halfopen connection detection

use futures;
use argh::FromArgs;
use tokio::task;
use tokio::time;

use std::time::Duration;

mod connection;

#[derive(FromArgs)]
/// Reach new heights.
struct Config {
    /// number of connections
    #[argh(option, short = 'c', default = "1")]
    connections: usize,

    /// size of payload
    #[argh(option, short = 'p', default = "1024")]
    payload: usize,

    /// number of messages
    #[argh(option, short = 'n', default = "10000")]
    count: u16,

    /// server
    #[argh(option, short = 's', default = "String::from(\"localhost\")")]
    server: String,

    /// port
    #[argh(option, short = 'P', default = "8883")]
    port : u16,

    /// keep_alive
    #[argh(option, short = 'k', default = "5")]
    keep_alive: u16,

    /// infligh_messages
    #[argh(option, short = 'q', default = "200")]
    inflight: u16,

    /// tls, 0, 1, 2. 0 -> no tls, 1 -> server verification, 2-> mTLS
    #[argh(option, short ='t', default = "0")]
    use_tls: i16,

    /// path to PEM encoded x509 ca-chain file
    #[argh(option, short='R')]
    ca_file: Option<String>,

    /// path to PEM encoded x509 client cert file.
    #[argh(option, short='C')]
    client_cert: Option<String>,

    /// path to PEM encoded client key file
    #[argh(option, short='K')]
    client_key: Option<String>,

    /// connection_timeout
    #[argh(option, short='T', default="5")]
    conn_timeout: u64,

    /// qos, default 1
    #[argh(option, short='Q', default="1")]
    qos: i16,
}

#[derive(Debug, Clone)]
pub struct Metrics {
    progress: u16,
}

#[tokio::main(core_threads = 4)]
async fn main() {
    pretty_env_logger::init();
    let config: Config = argh::from_env();
    let count = config.count;
    let payload_size = config.payload;
    let conns = config.connections;
    let server = config.server;
    let port  = config.port;
    let keep_alive = config.keep_alive;
    let inflight = config.inflight;
    let tls = config.use_tls;
    let ca_file = config.ca_file;
    let client_cert = config.client_cert;
    let client_key = config.client_key;
    let conn_timeout = config.conn_timeout;
    let qos = config.qos;

    let mut handles = vec![];
    for i in 0..conns{
        let srv = server.to_string();
        let cert = client_cert.to_owned();
        let key = client_key.to_owned();
        let chain = ca_file.to_owned();
        handles.push(task::spawn(async move {
            let id = format!("mqtt-{}", i);
            connection::start(&id, payload_size, count, srv,
                port, keep_alive, inflight, tls,
                chain, cert, key,
                conn_timeout, qos).await;
        }));
    }
    futures::future::join_all(handles).await;
}
