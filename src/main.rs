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

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate log;

use std::sync::Arc;

use argh::FromArgs;
use futures;
use futures::stream::StreamExt;
use async_channel;
use tokio::sync::Barrier;
use tokio::task;

mod connection;
use hdrhistogram::Histogram;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "mqttwrk", about = "A MQTT server bench marking tool inspired by wrk.")]
/// Reach new heights.
struct Config {
    /// number of connections
    #[structopt(short, long, default_value="1")]
    connections: usize,

    /// size of payload
    #[structopt(short="m", long, default_value="100")]
    payload_size: usize,

    /// number of messages
    #[structopt(short="n", long, default_value="100000")]
    count: usize,

    /// server
    #[structopt(short="h", long, default_value="localhost")]
    server: String,

    /// port
    #[structopt(short="p", long, default_value="1883")]
    port: u16,

    /// keep alive
    #[structopt(short="k", long, default_value="10")]
    keep_alive: u16,

    /// max inflight messages
    #[structopt(short="i", long, default_value="100")]
    max_inflight: u16,

    /// path to PEM encoded x509 ca-chain file
    #[structopt(short="R", long)]
    ca_file: Option<String>,

    /// path to PEM encoded x509 client cert file.
    #[structopt(short="C", long)]
    client_cert: Option<String>,

    /// path to PEM encoded client key file
    #[structopt(short="K", long)]
    client_key: Option<String>,

    /// connection_timeout
    #[structopt(short="t", long, default_value="5")]
    conn_timeout: u64,

    /// qos, default 1
    #[structopt(short="1", long, default_value="1")]
    qos: i16,

    /// number of publishers per connection, default 1
    // #[argh(option, short = 'x', default = "1")]
    #[structopt(short="x", long, default_value="1")]
    publishers: usize,

    /// number of subscribers per connection, default 1
    // #[argh(option, short = 'y', default = "0")]
    #[structopt(short="y", long, default_value="0")]
    subscribers: usize,

    /// sink connection 1
    // #[argh(option, short = 's')]
    #[structopt(short="s", long)]
    sink: Option<String>,

    /// delay in between each request in secs
    // #[argh(option, short = 'd', default = "0")]
    #[structopt(short="d", long, default_value="5")]
    delay: u64,
}

#[tokio::main(core_threads = 4)]
async fn main() {
    pretty_env_logger::init();

    let config: Config = Config::from_args();
    let config = Arc::new(config);
    let connections = if config.sink.is_some() {
        config.connections + 1
    } else {
        config.connections
    };
    let barrier = Arc::new(Barrier::new(connections));
    let mut handles = futures::stream::FuturesUnordered::new();
    let (tx, rx) = async_channel::bounded::<Histogram::<u64>>(config.connections);

    // We synchronously finish connections and subscriptions and then spawn
    // connection start to perform publishes concurrently.
    //
    // This simplifies 2 things
    // * Spawning too many connections wouldn't lead to `Elapsed` error
    //   in last spawns due to broker accepting connections sequentially
    // * We have to synchronize all subscription with a barrier because
    //   subscriptions shouldn't happen after publish to prevent wrong
    //   incoming publish count
    //
    // But the problem which doing connection synchronously (next connectoin
    // happens only after current connack is recived) is that remote connections
    // will take a long time to establish 10K connection (much greater than#[str]
    // 10K * 1 millisecond)
    for i in 0..config.connections {
        let mut connection = match connection::Connection::new(i, None, config.clone(), Some(tx.clone())).await {
            Ok(c) => c,
            Err(e) => {
                error!("Device = {}, Error = {:?}", i, e);
                return;
            }
        };

        let barrier = barrier.clone();
        handles.push(task::spawn(async move { connection.start(barrier).await }));
    }

    if let Some(filter) = config.sink.as_ref() {
        let mut connection =
            match connection::Connection::new(1, Some(filter.to_owned()), config.clone(), None).await {
                Ok(c) => c,
                Err(e) => {
                    error!("Device = sink-1, Error = {:?}", e);
                    return;
                }
            };

        let barrier = barrier.clone();
        handles.push(task::spawn(async move { connection.start(barrier).await }));
    }

    let mut cnt = 0;
    let mut hist = Histogram::<u64>::new(4).unwrap();

    loop {
        if handles.next().await.is_none() {
            break;
        }
        // TODO Collect histograms
        if let Ok(h) = rx.try_recv() {
            cnt += 1;
            hist.add(h).unwrap();
        }
        if cnt == config.connections{
            break;
        }
    }

    println!("-------------AGGREGATE-----------------");
    println!("# of samples          : {}", hist.len());
    println!("99.999'th percentile  : {}", hist.value_at_quantile(0.999999));
    println!("99.99'th percentile   : {}", hist.value_at_quantile(0.99999));
    println!("90 percentile         : {}", hist.value_at_quantile(0.90));
    println!("50 percentile         : {}", hist.value_at_quantile(0.5));
}
