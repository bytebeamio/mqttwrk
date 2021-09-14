//! Tool to vet mqtt brokers. The goal here is to test and benchmark
//! mqtt brokers for features, robustness, performance and scalability
//!
//! Goals of the test suite
//!
//! - Spawn n clients with publish and subscribe on the same topic (and report thoughput and latencies)
//! - Spawn n clinets with publishes and 1 subscription to pull all the data (used to simulate a sink in the cloud)

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate log;
#[macro_use]
extern crate colour;

mod bench;
mod test;

use pprof::{protos::Message, ProfilerGuard};
use std::io::Write;
use structopt::StructOpt;
use std::fs;

#[derive(Debug, StructOpt)]
#[structopt(
name = "mqttwrk",
about = "A MQTT server bench marking tool inspired by wrk."
)]
enum Config {
    Bench(BenchConfig),
    Test,
}

#[derive(Debug, StructOpt)]
struct BenchConfig {
    // number of publishers
    #[structopt(long = "pub_n", default_value = "1")]
    publishers: usize,
    // number of subscribers
    #[structopt(long = "sub_n", default_value = "1")]
    subscribers: usize,
    /// size of payload
    #[structopt(short = "m", long, default_value = "100")]
    payload_size: usize,
    /// number of messages (n = 0 is for idle connection to test pings)
    #[structopt(short = "n", long, default_value = "100")]
    count: usize,
    /// server
    #[structopt(short = "h", long, default_value = "localhost")]
    server: String,
    /// port
    #[structopt(short = "p", long, default_value = "1883")]
    port: u16,
    /// keep alive
    #[structopt(short = "k", long, default_value = "10")]
    keep_alive: u16,
    /// max inflight messages
    #[structopt(short = "i", long, default_value = "100")]
    max_inflight: u16,
    /// path to PEM encoded x509 ca-chain file
    #[structopt(short = "R", long)]
    ca_file: Option<String>,
    /// path to PEM encoded x509 client cert file.
    #[structopt(short = "C", long)]
    client_cert: Option<String>,
    /// path to PEM encoded client key file
    #[structopt(short = "K", long)]
    client_key: Option<String>,
    /// connection_timeout
    #[structopt(short = "t", long, default_value = "5")]
    conn_timeout: u64,
    /// qos, default 0
    #[structopt(long = "pub_q", default_value = "0")]
    publish_qos: i16,
    /// qos, default 0
    #[structopt(long = "sub_q", default_value = "0")]
    subscribe_qos: i16,
    /// delay in between each request in secs
    #[structopt(short = "d", long, default_value = "0")]
    delay: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    pretty_env_logger::init();
    let config: Config = Config::from_args();
    match config {
        Config::Bench(config) => {
            let guard = pprof::ProfilerGuard::new(100).unwrap();
            bench::start(config).await;
            profile("bench.pb", guard);
        },
        Config::Test => {
            test::start().await;
        }
    }
}

#[allow(unused)]
pub fn profile(name: &str, guard: ProfilerGuard) {
    if let Ok(report) = guard.report().build() {
        let mut file = fs::File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode(&mut content).unwrap();
        file.write_all(&content).unwrap();
    };
}
