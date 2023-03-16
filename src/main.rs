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
    Simulator(SimulatorConfig),
    Round(RoundConfig),
    Conformance(ConformanceConfig),
    Test,
}

#[derive(Debug, Parser)]
struct BenchConfig {
    #[arg(short = 'S', long, default_value = "localhost", value_name = "URL")]
    server: String,
    #[arg(short = 'P', long, default_value = "1883")]
    port: u16,

    #[arg(short = 'p', long, default_value = "1", value_name = "NUM")]
    publishers: usize,
    #[arg(short = 's', long, default_value = "0", value_name = "NUM")]
    subscribers: usize,

    #[arg(long, default_value = "0", value_name = "QoS")]
    publish_qos: i16,
    #[arg(long, default_value = "0", value_name = "QoS")]
    subscribe_qos: i16,

    #[arg(short = 'n', long, default_value = "100", value_name = "NUM")]
    count: usize,
    #[arg(short = 'r', long, default_value = "0")]
    rate: u64,
    #[arg(short = 'm', long, default_value = "100")]
    payload_size: usize,

    #[arg(
        long,
        default_value = "{unique_id}/hello/{pub_id}/world",
        long_help = "\
Topic format to which data is published to.
When present:
    `{pub_id}` is replaced by publisher_id
    `{unique_id}` is replaced with a randomly generated string.
This is useful to uniquely identify different runs of benchmark.
"
    )]
    topic_format: String,
    #[arg(
        long,
        default_value_t = true,
        long_help = "\
If true, prefixes a unique_id to client_id of each publisher and subsciber. 
Same as `{unique_id}` in `topic_format`.
"
    )]
    unique_client_id_prefix: bool,

    #[arg(short = 'k', long, default_value = "10")]
    keep_alive: u64,
    #[arg(short = 'i', long, default_value = "100")]
    max_inflight: u16,
    #[arg(short = 't', long, default_value = "10")]
    conn_timeout: u64,

    #[arg(short = 'R', long)]
    ca_file: Option<String>,

    #[arg(long, default_value = "false")]
    show_pub_stat: bool,
    #[arg(long, default_value = "false")]
    show_sub_stat: bool,

    #[arg(long, default_value = "0")]
    sleep_sub: u64,
}

#[derive(Debug, Parser)]
struct SimulatorConfig {
    #[arg(short = 'S', long, default_value = "localhost", value_name = "URL")]
    server: String,
    #[arg(short = 'P', long, default_value = "1883")]
    port: u16,

    #[arg(short = 'p', long, default_value = "1", value_name = "NUM")]
    publishers: usize,
    #[arg(short = 's', long, default_value = "0", value_name = "NUM")]
    subscribers: usize,

    #[arg(long, default_value = "0", value_name = "QoS")]
    publish_qos: i16,
    #[arg(long, default_value = "0", value_name = "QoS")]
    subscribe_qos: i16,

    #[arg(short = 'n', long, default_value = "100", value_name = "NUM")]
    count: usize,
    #[arg(short = 'r', long, default_value = "0")]
    rate: u64,
    #[arg(short = 'm', long, default_value = "100")]
    payload_size: usize,
    #[arg(long, value_enum)]
    data_type: DataType,

    #[arg(
        long,
        default_value = "/tenants/demo/devices/{pub_id}/events/{data_type}/jsonarray",
        long_help = "\
Topic format to which data is published to.
When present:
    `{pub_id}` is replaced by publisher_id
    `{unique_id}` is replaced with a randomly generated string.
This is useful to uniquely identify different runs of benchmark.
"
    )]
    topic_format: String,
    #[arg(
        long,
        default_value_t = true,
        long_help = "\
If true, prefixes a unique_id to client_id of each publisher and subsciber. 
Same as `{unique_id}` in `topic_format`.
"
    )]
    unique_client_id_prefix: bool,

    #[arg(short = 'k', long, default_value = "10")]
    keep_alive: u64,
    #[arg(short = 'i', long, default_value = "100")]
    max_inflight: u16,
    #[arg(short = 't', long, default_value = "10")]
    conn_timeout: u64,

    #[arg(short = 'R', long)]
    ca_file: Option<String>,

    #[arg(long, default_value = "false")]
    show_pub_stat: bool,
    #[arg(long, default_value = "false")]
    show_sub_stat: bool,

    #[arg(long, default_value = "0")]
    sleep_sub: u64,
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    server: String,
    port: u16,
    publishers: usize,
    subscribers: usize,
    publish_qos: i16,
    subscribe_qos: i16,
    count: usize,
    rate: u64,
    payload: DataType,
    topic_format: String,
    unique_client_id_prefix: bool,
    keep_alive: u64,
    max_inflight: u16,
    conn_timeout: u64,
    ca_file: Option<String>,
    show_pub_stat: bool,
    show_sub_stat: bool,
    sleep_sub: u64,
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
pub struct ConformanceConfig {
    /// Broker's address
    #[arg(short = 'S', long, default_value = "localhost", value_name = "URL")]
    server: String,
    /// Port
    #[arg(short = 'P', long, default_value = "1883")]
    port: u16,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum DataType {
    #[value(skip)]
    Default(usize),
    Imu,
    Bms,
    Gps,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default(_) => f.write_str("default"),
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
            bench::start(config);
        }
        Config::Round(config) => {
            round::start(config).unwrap();
        }
        Config::Conformance(config) => {
            conformance::start(config);
        }
        Config::Test => {
            test::start();
        }
    }
}
