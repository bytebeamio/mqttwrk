//! Tool to vet mqtt brokers. The goal here is to test and benchmark
//! mqtt brokers for features, robustness, performance and scalability
//!
//! Goals of the test suite
//!
//! - Spawn n clients with publish and subscribe on the same topic (and report thoughput and latencies)
//! - Spawn n clinets with publishes and 1 subscription to pull all the data (used to simulate a sink in the cloud)
//! - Offline messaging
//! - Halfopen connection detection

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
    inflight: usize,

    // ///use mTLS
    // #[argh(option, short = 't', default = "false")]
    // tls: bool,

    // /// certs dir
    // #[argh(option, short = 'd')]
    // cert_dir: Option<String>,
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
    // let cert_dir = config.cert_dir;
    // let tls = config.tls;
    let port  = config.port;
    let keep_alive = config.keep_alive;
    let inflight = config.inflight;

    for i in 0..conns{
        let srv = server.to_string();
        task::spawn(async move {
            let id = format!("mqtt-{}", i);
            connection::start(&id, payload_size, count, srv, port, keep_alive, inflight).await;
            //&conn_config.do_something().await;
        });
    }
    

    time::delay_for(Duration::from_secs(100)).await;
}
