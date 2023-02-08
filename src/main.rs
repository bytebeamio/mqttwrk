//! Tool to vet mqtt brokers. The goal here is to test and benchmark
//! mqtt brokers for features, robustness, performance and scalability
//!
//! Goals of the test suite
//!
//! - Spawn n clients with publish and subscribe on the same topic (and report thoughput and latencies)
//! - Spawn n clinets with publishes and 1 subscription to pull all the data (used to simulate a sink in the cloud)

use std::fmt::Display;

use clap::{Parser, ValueEnum};

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

#[derive(Debug, Parser)]
#[command(
    name = "mqttwrk",
    about = "A MQTT server benchmarking tool inspired by wrk.",
    version
)]
enum Config {
    Bench(BenchConfig),
    Round(RoundConfig),
    Simulator(SimulatorConfig),
    Conformance,
    Test,
}

#[derive(Debug, Parser)]
struct BenchConfig {
    /// Broker's address
    #[arg(short = 'S', long, default_value = "localhost", value_name = "URL")]
    server: String,
    /// Port
    #[arg(short = 'P', long, default_value = "1883")]
    port: u16,
    /// No. of messages per publisher (n = 0 is for idle connection to test pings)
    #[arg(short = 'n', long, default_value = "100", value_name = "NUM")]
    count: usize,
    /// No. of Publishers
    #[arg(short = 'p', long, default_value = "1", value_name = "NUM")]
    publishers: usize,
    /// No. of Subscribers
    #[arg(short = 's', long, default_value = "0", value_name = "NUM")]
    subscribers: usize,
    /// QoS used for Publishes
    #[arg(long, default_value = "0", value_name = "QoS")]
    publish_qos: i16,
    /// Payload size in Bytes
    #[arg(short = 'm', long, default_value = "100")]
    payload_size: usize,
    /// QoS used by Subscriber
    #[arg(long, default_value = "0", value_name = "QoS")]
    subscribe_qos: i16,
    /// Keep Alive
    #[arg(short = 'k', long, default_value = "10")]
    keep_alive: u64,
    /// Max Inflight Messages
    #[arg(short = 'i', long, default_value = "100")]
    max_inflight: u16,
    /// Path to PEM encoded x509 ca-chain file
    #[arg(short = 'R', long)]
    ca_file: Option<String>,
    /// Connection Timeout
    #[arg(short = 't', long, default_value = "10")]
    conn_timeout: u64,
    /// Message rate per second. (0 means no throttle)
    #[arg(short = 'r', long, default_value = "0")]
    rate: u64,
    /// Show publisher stats
    #[arg(long, default_value = "false")]
    show_pub_stat: bool,
    /// Show subscriber stats
    #[arg(long, default_value = "false")]
    show_sub_stat: bool,
}

#[derive(Clone, Debug, Parser)]
struct RoundConfig {
    #[arg(short = 'c', long = "connections")]
    #[allow(dead_code)]
    connections: Option<usize>,
    #[arg(short = 'i', long = "in-flight", default_value = "100")]
    in_flight: usize,
    #[arg(short = 'b', long = "broker", default_value = "localhost")]
    broker: String,
    #[arg(short = 'p', long = "port", default_value = "1883")]
    port: u16,
    #[arg(short = 's', long = "payload-size", default_value = "100")]
    payload_size: usize,
    #[arg(short = 'd', long = "duration", default_value = "10")]
    duration: u64,
    #[arg(short = 'n', long = "count")]
    max_publishes: Option<u64>,
}

#[derive(Debug, Parser)]
struct SimulatorConfig {
    /// default topic format to which data is published to.
    /// if present:
    ///     `{pub_id}` is replaced by publisher_id
    ///     `{data_type}` is replace by type of data being published
    #[arg(
        long,
        default_value = "/tenants/demo/devices/{pub_id}/events/{data_type}/jsonarray"
    )]
    topic_format: String,
    /// number of messages (n = 0 is for idle connection to test pings)
    #[arg(short = 'n', long, default_value = "100")]
    count: usize,
    /// server
    #[arg(short = 'S', long, default_value = "localhost")]
    server: String,
    /// port
    #[arg(short = 'P', long, default_value = "1883")]
    port: u16,
    /// number of publishers
    #[arg(short = 'p', default_value = "1")]
    publishers: usize,
    /// number of subscribers
    #[arg(short = 's', default_value = "0")]
    subscribers: usize,
    /// qos, default 1
    #[arg(short = 'x', default_value = "1")]
    publish_qos: i16,
    /// qos, default 1
    #[arg(short = 'y', default_value = "1")]
    subscribe_qos: i16,
    /// size of payload
    /// keep alive
    #[arg(short = 'k', long, default_value = "10")]
    keep_alive: u64,
    /// max inflight messages
    #[arg(short = 'i', long, default_value = "100")]
    max_inflight: u16,
    /// path to PEM encoded x509 ca-chain file
    #[arg(short = 'R', long)]
    ca_file: Option<String>,
    /// connection_timeout
    #[arg(short = 't', long, default_value = "5")]
    conn_timeout: u64,
    /// message rate. 0 => no throttle
    #[arg(long, default_value = "0")]
    rate_pub: u64,
    #[arg(long, default_value = "0")]
    sleep_sub: u64,
    /// Show publisher stats
    #[arg(long, default_value = "false")]
    show_pub_stat: bool,
    /// Show subscriber stats
    #[arg(long, default_value = "false")]
    show_sub_stat: bool,
    /// Type of data to send
    #[arg(long, value_enum)]
    data_type: DataType,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum DataType {
    Imu,
    Bms,
    Gps,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Imu => f.write_str("imu"),
            Self::Bms => f.write_str("bms"),
            Self::Gps => f.write_str("gps"),
        }
    }
}

fn main() {
    pretty_env_logger::init();
    let config: Config = Config::parse();
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
