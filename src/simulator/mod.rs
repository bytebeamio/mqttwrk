use std::{fs, io, sync::Arc, time::Duration};

use futures::StreamExt;
use indicatif::ProgressBar;
use rumqttc::{MqttOptions, QoS, Transport};
use tokio::{sync::Barrier, task};

use crate::{
    common::{PubStats, Stats, SubStats, PROGRESS_STYLE},
    SimulatorConfig,
};

mod publisher;
mod subscriber;

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("IO error = {0:?}")]
    Io(#[from] io::Error),
    #[error("Connection error = {0:?}")]
    Connection(#[from] rumqttc::ConnectionError),
    #[error("Wrong packet = {0:?}")]
    WrongPacket(rumqttc::Incoming),
    #[error("Client error = {0:?}")]
    Client(#[from] rumqttc::ClientError),
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub(crate) async fn start(config: SimulatorConfig) {
    let config = Arc::new(config);
    let mut handles = futures::stream::FuturesUnordered::new();
    let barrier_sub = Arc::new(Barrier::new(config.subscribers));
    let barrier_pub = Arc::new(Barrier::new(config.publishers));

    let sub_bar = ProgressBar::new(config.subscribers as u64)
        .with_prefix("Subscribers Spawned:")
        .with_style((*PROGRESS_STYLE).clone());

    // spawning subscribers
    for i in 0..config.subscribers {
        let config = Arc::clone(&config);
        let id = format!("sub-{i:05}");
        let barrier_handle = barrier_sub.clone();
        sub_bar.set_message(format!("spawning {id}"));
        let mut subscriber = subscriber::Subscriber::new(id, config).await.unwrap();
        handles.push(task::spawn(async move {
            Stats::SubStats(subscriber.start(barrier_handle).await)
        }));
        sub_bar.inc(1);
    }
    sub_bar.finish_with_message("Done!");

    // spawing publishers
    let pub_bar = ProgressBar::new(config.publishers as u64)
        .with_prefix("Publishers Spawned:")
        .with_style((*PROGRESS_STYLE).clone());

    for i in 0..config.publishers {
        let config = Arc::clone(&config);
        let id = format!("pub-{i:05}");
        let barrier_handle = barrier_pub.clone();
        pub_bar.set_message(format!("spawning {id}"));
        let mut publisher = publisher::Publisher::new(id, config).await.unwrap();
        handles.push(task::spawn(async move {
            Stats::PubStats(publisher.start(barrier_handle).await)
        }));
        pub_bar.inc(1);
    }
    pub_bar.finish_with_message("Done!");

    let mut aggregate_substats = SubStats::default();
    let mut aggregate_pubstats = PubStats::default();
    // await and consume all futures
    while let Some(some_stat) = handles.next().await {
        match some_stat.unwrap() {
            Stats::SubStats(substats) => {
                aggregate_substats.publish_count += substats.publish_count;
                aggregate_substats.puback_count += substats.puback_count;
                aggregate_substats.reconnects += substats.reconnects;
                aggregate_substats.throughput += substats.throughput;
            }
            Stats::PubStats(pubstats) => {
                aggregate_pubstats.outgoing_publish += pubstats.outgoing_publish;
                aggregate_pubstats.throughput += pubstats.throughput;
                aggregate_pubstats.reconnects += pubstats.reconnects;
            }
        }
    }

    println!(
        "Aggregate PubStats: {:#?}\nAggregate SubStats: {:#?}",
        &aggregate_pubstats, &aggregate_substats
    );
}

pub(crate) fn options(config: Arc<SimulatorConfig>, id: &str) -> io::Result<MqttOptions> {
    let mut options = MqttOptions::new(id, &config.server, config.port);
    options.set_keep_alive(Duration::from_secs(config.keep_alive));
    options.set_inflight(config.max_inflight);

    if let Some(ca_file) = &config.ca_file {
        let ca = fs::read(ca_file)?;
        options.set_transport(Transport::tls(ca, None, None));
    }

    Ok(options)
}

/// get QoS level. Default is AtLeastOnce.
fn get_qos(qos: i16) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::AtLeastOnce,
    }
}
