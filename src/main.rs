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

mod bench;

use pprof::{protos::Message, ProfilerGuard};
use rumqttc::*;
use std::io::Write;
use std::{fs, io};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "mqttwrk",
    about = "A MQTT server bench marking tool inspired by wrk."
)]
struct Config {
    /// number of connections
    #[structopt(short, long, default_value = "1")]
    connections: usize,
    /// size of payload
    #[structopt(short = "m", long, default_value = "100")]
    payload_size: usize,
    /// number of messages
    #[structopt(short = "n", long, default_value = "1000000")]
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
    /// qos, default 1
    #[structopt(short = "1", long, default_value = "1")]
    qos: i16,
    /// number of publishers per connection, default 1
    #[structopt(short = "x", long, default_value = "1")]
    publishers: usize,
    /// number of subscribers per connection, default 1
    #[structopt(short = "y", long, default_value = "0")]
    subscribers: usize,
    /// sink connection 1
    #[structopt(short = "s", long, default_value = "1")]
    sink: usize,
    /// delay in between each request in secs
    #[structopt(short = "d", long, default_value = "0")]
    delay: u64,
}

impl Config {
    pub fn options(&self, id: &str) -> io::Result<MqttOptions> {
        let mut options = MqttOptions::new(id, &self.server, self.port);
        options.set_keep_alive(self.keep_alive);
        options.set_inflight(self.max_inflight);
        options.set_connection_timeout(self.conn_timeout);

        if let Some(ca_file) = &self.ca_file {
            let ca = fs::read(ca_file)?;

            let client_auth = match &self.client_cert {
                Some(f) => {
                    let cert = fs::read(f)?;
                    let key = fs::read(&self.client_key.as_ref().unwrap())?;
                    Some((cert, Key::RSA(key)))
                }
                None => None,
            };

            options.set_transport(Transport::tls(ca, client_auth, None));
        }

        Ok(options)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    pretty_env_logger::init();
    let config: Config = Config::from_args();
    let guard = pprof::ProfilerGuard::new(100).unwrap();

    bench::start(config).await;
    profile("bench.pb", guard);
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
