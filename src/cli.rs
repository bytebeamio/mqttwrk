use std::fmt::Display;

use clap::{Args, Parser, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "mqttwrk",
    about = "A MQTT server benchmarking tool inspired by wrk.",
    version
)]
pub enum Cli {
    Bench(BenchConfig),
    Simulator(SimulatorConfig),
    Round(RoundConfig),
    Conformance(ConformanceConfig),
    Test,
}

#[derive(Debug, Parser)]
pub struct BenchConfig {
    #[command(flatten)]
    network_config: _NetworkConfig,

    #[command(flatten)]
    common_config: _CommonConfig,

    #[arg(short = 'm', long, default_value = "100")]
    pub payload_size: usize,
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
    pub topic_format: String,
}

#[derive(Debug, Parser)]
pub struct SimulatorConfig {
    #[command(flatten)]
    network_config: _NetworkConfig,

    #[command(flatten)]
    common_config: _CommonConfig,

    #[arg(short = 'm', long, default_value = "100")]
    pub payload_size: usize,

    #[arg(long, value_enum)]
    pub data_type: DataType,

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
    pub topic_format: String,
}

#[derive(Debug, Clone, Args)]
struct _CommonConfig {
    #[arg(short = 'p', long, default_value = "1", value_name = "NUM")]
    pub publishers: usize,
    #[arg(short = 's', long, default_value = "0", value_name = "NUM")]
    pub subscribers: usize,

    #[arg(long, default_value = "0", value_name = "QoS")]
    pub publish_qos: i16,
    #[arg(long, default_value = "0", value_name = "QoS")]
    pub subscribe_qos: i16,

    #[arg(short = 'n', long, default_value = "100", value_name = "NUM")]
    pub count: usize,
    #[arg(short = 'r', long, default_value = "0")]
    pub rate: u64,

    #[arg(
        long,
        default_value_t = true,
        long_help = "\
If true, prefixes a unique_id to client_id of each publisher and subsciber. 
Same as `{unique_id}` in `topic_format`.
"
    )]
    pub disable_unique_clientid_prefix: bool,

    #[arg(short = 'R', long)]
    pub ca_file: Option<String>,

    #[arg(long, default_value = "false")]
    pub show_pub_stat: bool,
    #[arg(long, default_value = "false")]
    pub show_sub_stat: bool,

    #[arg(long, default_value = "0")]
    pub sleep_sub: u64,
}

#[derive(Debug, Clone, Args)]
struct _NetworkConfig {
    #[arg(short = 'S', long, default_value = "localhost", value_name = "URL")]
    pub server: String,
    #[arg(short = 'P', long, default_value = "1883")]
    pub port: u16,

    #[arg(short = 'k', long, default_value = "10")]
    pub keep_alive: u64,
    #[arg(short = 'i', long, default_value = "100")]
    pub max_inflight: u16,
    #[arg(short = 't', long, default_value = "10")]
    pub conn_timeout: u64,
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub server: String,
    pub port: u16,
    pub publishers: usize,
    pub subscribers: usize,
    pub publish_qos: i16,
    pub subscribe_qos: i16,
    pub count: usize,
    pub rate: u64,
    pub payload: DataType,
    pub topic_format: String,
    pub disable_unqiue_clientid_prefix: bool,
    pub keep_alive: u64,
    pub max_inflight: u16,
    pub conn_timeout: u64,
    pub ca_file: Option<String>,
    pub show_pub_stat: bool,
    pub show_sub_stat: bool,
    pub sleep_sub: u64,
}

#[derive(Clone, Debug, Parser)]
pub struct RoundConfig {
    #[arg(short = 'c', long = "connections")]
    #[allow(dead_code)]
    pub connections: Option<usize>,
    #[arg(short = 'i', long = "in-flight", default_value = "100")]
    pub in_flight: usize,
    #[arg(short = 'b', long = "broker", default_value = "localhost")]
    pub broker: String,
    #[arg(short = 'p', long = "port", default_value = "1883")]
    pub port: u16,
    #[arg(short = 's', long = "payload-size", default_value = "100")]
    pub payload_size: usize,
    #[arg(short = 'd', long = "duration", default_value = "10")]
    pub duration: u64,
    #[arg(short = 'n', long = "count")]
    pub max_publishes: Option<u64>,
}

#[derive(Debug, Parser)]
pub struct ConformanceConfig {
    /// Broker's address
    #[arg(short = 'S', long, default_value = "localhost", value_name = "URL")]
    pub server: String,
    /// Port
    #[arg(short = 'P', long, default_value = "1883")]
    pub port: u16,
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

impl From<SimulatorConfig> for RunnerConfig {
    fn from(value: SimulatorConfig) -> Self {
        match value.data_type {
            DataType::Default(_) => {
                panic!("Shouldn't specify payload size when specifying data_type")
            }
            _ => {}
        };

        Self {
            server: value.network_config.server,
            port: value.network_config.port,
            publishers: value.common_config.publishers,
            subscribers: value.common_config.subscribers,
            publish_qos: value.common_config.publish_qos,
            subscribe_qos: value.common_config.subscribe_qos,
            count: value.common_config.count,
            rate: value.common_config.rate,
            payload: value.data_type,
            topic_format: value.topic_format,
            disable_unqiue_clientid_prefix: value.common_config.disable_unique_clientid_prefix,
            keep_alive: value.network_config.keep_alive,
            max_inflight: value.network_config.max_inflight,
            conn_timeout: value.network_config.conn_timeout,
            ca_file: value.common_config.ca_file,
            show_pub_stat: value.common_config.show_pub_stat,
            show_sub_stat: value.common_config.show_sub_stat,
            sleep_sub: value.common_config.sleep_sub,
        }
    }
}

impl From<BenchConfig> for RunnerConfig {
    fn from(value: BenchConfig) -> Self {
        let payload = DataType::Default(value.payload_size);
        Self {
            server: value.network_config.server,
            port: value.network_config.port,
            publishers: value.common_config.publishers,
            subscribers: value.common_config.subscribers,
            publish_qos: value.common_config.publish_qos,
            subscribe_qos: value.common_config.subscribe_qos,
            count: value.common_config.count,
            rate: value.common_config.rate,
            payload,
            topic_format: value.topic_format,
            disable_unqiue_clientid_prefix: value.common_config.disable_unique_clientid_prefix,
            keep_alive: value.network_config.keep_alive,
            max_inflight: value.network_config.max_inflight,
            conn_timeout: value.network_config.conn_timeout,
            ca_file: value.common_config.ca_file,
            show_pub_stat: value.common_config.show_pub_stat,
            show_sub_stat: value.common_config.show_sub_stat,
            sleep_sub: value.common_config.sleep_sub,
        }
    }
}