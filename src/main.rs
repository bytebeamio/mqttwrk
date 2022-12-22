//! Tool to vet mqtt brokers. The goal here is to test and benchmark
//! mqtt brokers for features, robustness, performance and scalability
//!
//! Goals of the test suite
//!
//! - Spawn n clients with publish and subscribe on the same topic (and report thoughput and latencies)
//! - Spawn n clinets with publishes and 1 subscription to pull all the data (used to simulate a sink in the cloud)

#[macro_use]
extern crate log;
#[macro_use]
extern crate colour;

mod bench;
mod common;
mod conformance;
mod round;
mod simulator;
mod test;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "mqttwrk",
    about = "A MQTT server benchmarking tool inspired by wrk."
)]
enum Config {
    Bench(BenchConfig),
    Round(RoundConfig),
    Simulator(SimulatorConfig),
    Conformance,
    Test,
}

#[derive(Debug, StructOpt)]
struct BenchConfig {
    #[structopt(short = "m", long, default_value = "100")]
    payload_size: usize,
    /// number of messages (n = 0 is for idle connection to test pings)
    #[structopt(short = "n", long, default_value = "100")]
    count: usize,
    /// server
    #[structopt(short = "S", long, default_value = "localhost")]
    server: String,
    /// port
    #[structopt(short = "P", long, default_value = "1883")]
    port: u16,
    /// number of publishers
    #[structopt(short = "p", default_value = "1")]
    publishers: usize,
    /// number of subscribers
    #[structopt(short = "s", default_value = "0")]
    subscribers: usize,
    /// qos, default 0
    #[structopt(short = "x", default_value = "1")]
    publish_qos: i16,
    /// qos, default 0
    #[structopt(short = "y", default_value = "1")]
    subscribe_qos: i16,
    /// size of payload
    /// keep alive
    #[structopt(short = "k", long, default_value = "10")]
    keep_alive: u64,
    /// max inflight messages
    #[structopt(short = "i", long, default_value = "100")]
    max_inflight: u16,
    /// path to PEM encoded x509 ca-chain file
    #[structopt(short = "R", long)]
    ca_file: Option<String>,
    /// connection_timeout
    #[structopt(short = "t", long, default_value = "5")]
    conn_timeout: u64,
    /// message rate. 0 => no throttle
    #[structopt(short = "r", long, default_value = "0")]
    rate: u64,
}

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "mqttround")]
struct RoundConfig {
    #[structopt(short = "c", long = "connections")]
    #[allow(dead_code)]
    connections: Option<usize>,
    #[structopt(short = "i", long = "in-flight", default_value = "100")]
    in_flight: usize,
    #[structopt(short = "b", long = "broker", default_value = "localhost")]
    broker: String,
    #[structopt(short = "p", long = "port", default_value = "1883")]
    port: u16,
    #[structopt(short = "s", long = "payload-size", default_value = "100")]
    payload_size: usize,
    #[structopt(short = "d", long = "duration", default_value = "10")]
    duration: u64,
    #[structopt(short = "n", long = "count")]
    max_publishes: Option<u64>,
}

#[derive(Debug, StructOpt)]
struct SimulatorConfig {
    /// default topic format to which data is published to. `{}` is replaced by publisher_id
    #[structopt(long, default_value = "/tenants/demo/devices/{}/events/imu/jsonarray")]
    topic_format: String,
    /// number of messages (n = 0 is for idle connection to test pings)
    #[structopt(short = "n", long, default_value = "100")]
    count: usize,
    /// server
    #[structopt(short = "S", long, default_value = "localhost")]
    server: String,
    /// port
    #[structopt(short = "P", long, default_value = "1883")]
    port: u16,
    /// number of publishers
    #[structopt(short = "p", default_value = "1")]
    publishers: usize,
    /// number of subscribers
    #[structopt(short = "s", default_value = "0")]
    subscribers: usize,
    /// qos, default 0
    #[structopt(short = "x", default_value = "1")]
    publish_qos: i16,
    /// qos, default 0
    #[structopt(short = "y", default_value = "1")]
    subscribe_qos: i16,
    /// size of payload
    /// keep alive
    #[structopt(short = "k", long, default_value = "10")]
    keep_alive: u64,
    /// max inflight messages
    #[structopt(short = "i", long, default_value = "100")]
    max_inflight: u16,
    /// path to PEM encoded x509 ca-chain file
    #[structopt(short = "R", long)]
    ca_file: Option<String>,
    /// connection_timeout
    #[structopt(short = "t", long, default_value = "5")]
    conn_timeout: u64,
    /// message rate. 0 => no throttle
    #[structopt(long, default_value = "0")]
    rate_pub: u64,
    #[structopt(long, default_value = "0")]
    sleep_sub: u64,
}

fn main() {
    pretty_env_logger::init();
    let config: Config = Config::from_args();
    match config {
        Config::Bench(config) => {
            bench::start(config);
        }
        Config::Simulator(config) => {
            simulator::start(config);
        }
        Config::Round(config) => {
            round::start(config).unwrap();
        }
        Config::Conformance => {
            conformance::start();
        }
        Config::Test => {
            test::start();
        }
    }
}
