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

    for i in 0..config.connections {
        task::spawn(async move {
            let id = format!("mqtt-{}", i);
            connection::start(&id, payload_size, count).await;
        });
    }

    time::delay_for(Duration::from_secs(100)).await;
}
