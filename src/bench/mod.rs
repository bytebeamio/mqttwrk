use std::{fs, io, sync::Arc, time::Duration};

use futures::future::join;
use indicatif::{MultiProgress, ProgressBar};
use rumqttc::{MqttOptions, QoS, Transport};
use tokio::{sync::Barrier, task::JoinSet, time::Instant};

use crate::{
    cli::RunnerConfig,
    common::{PubStats, SubStats, PROGRESS_STYLE, UNIQUE_ID},
};

mod gendata;
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

// Simulator is a special kind of Bench
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub(crate) async fn start(config: impl Into<RunnerConfig>) {
    let config = Arc::new(config.into());
    let progress_bar = MultiProgress::new();
    let pub_bar = progress_bar.add(
        ProgressBar::new(config.publishers as u64)
            .with_prefix("Publishers Spawned:")
            .with_style((*PROGRESS_STYLE).clone()),
    );
    pub_bar.enable_steady_tick(Duration::from_secs_f64(0.1));

    let sub_bar = progress_bar.add(
        ProgressBar::new(config.subscribers as u64)
            .with_prefix("Subscribers Spawned:")
            .with_style((*PROGRESS_STYLE).clone()),
    );
    sub_bar.enable_steady_tick(Duration::from_secs_f64(0.1));

    let mut handles_sub = JoinSet::new();
    let mut handles_pub = JoinSet::new();
    let barrier_sub = Arc::new(Barrier::new(config.subscribers + 1));
    let barrier_pub = Arc::new(Barrier::new(config.publishers + 1));

    let unique_id = if config.disable_unqiue_clientid_prefix {
        "".to_string()
    } else {
        let id = &(*UNIQUE_ID);
        format!("-{id}")
    };

    // spawning subscribers
    for i in 0..config.subscribers {
        let id = format!("sub{unique_id}-{i:05}");
        let barrier_handle = barrier_sub.clone();

        sub_bar.set_message(format!("spawning {id}"));
        let mut subscriber = subscriber::Subscriber::new(id.clone(), Arc::clone(&config))
            .await
            .unwrap();

        handles_sub
            .build_task()
            .name(&format!("{}", id))
            .spawn(async move { subscriber.start(barrier_handle).await })
            .unwrap();
        sub_bar.inc(1);
    }

    let barrier_sub_handle = barrier_sub.clone();
    barrier_sub_handle.wait().await;
    sub_bar.finish_with_message("Done!");

    for i in 0..config.publishers {
        let id = format!("pub{unique_id}-{i:05}");
        let barrier_handle = barrier_pub.clone();

        pub_bar.set_message(format!("spawning {id}"));
        let publisher = publisher::Publisher::new(id.clone(), Arc::clone(&config))
            .await
            .unwrap();

        handles_pub
            .build_task()
            .name(&format!("{}", id))
            .spawn(async move { publisher.start(barrier_handle).await })
            .unwrap();
        pub_bar.inc(1);
    }
    let barrier_pub_handle = barrier_pub.clone();
    barrier_pub_handle.wait().await;
    pub_bar.finish_with_message("Done!");

    let test_start_time = Instant::now();

    let (aggregate_pubstats, aggregate_substats) = join(
        handle_pubs(handles_pub, test_start_time),
        // Passing pub_start_time to subscribers to calculate throughput because it is the time
        // from which publishers start publishing
        handle_subs(handles_sub, test_start_time),
    )
    .await;

    println!(
        "Aggregate PubStats: {:#?}\nAggregate SubStats: {:#?}",
        &aggregate_pubstats, &aggregate_substats
    );
}

pub async fn handle_subs(
    mut handles_sub: JoinSet<SubStats>,
    start_time: Instant,
) -> (SubStats, f64) {
    let mut aggregate_substats = SubStats::default();
    while let Some(Ok(sub_stat)) = handles_sub.join_next().await {
        aggregate_substats.publish_count += sub_stat.publish_count;
        aggregate_substats.puback_count += sub_stat.puback_count;
        aggregate_substats.reconnects += sub_stat.reconnects;
        aggregate_substats.throughput += sub_stat.throughput;
    }

    let total_messages = aggregate_substats.publish_count;
    let sub_time = start_time.elapsed().as_secs_f64();
    let throughput = total_messages as f64 / sub_time;

    (aggregate_substats, throughput)
}

pub async fn handle_pubs(
    mut handles_pub: JoinSet<PubStats>,
    start_time: Instant,
) -> (PubStats, f64) {
    let mut aggregate_pubstats = PubStats::default();

    while let Some(Ok(pub_stat)) = handles_pub.join_next().await {
        aggregate_pubstats.outgoing_publish += pub_stat.outgoing_publish;
        aggregate_pubstats.throughput += pub_stat.throughput;
        aggregate_pubstats.reconnects += pub_stat.reconnects;
    }

    let total_messages = aggregate_pubstats.outgoing_publish;
    let pub_time = start_time.elapsed().as_secs_f64();
    let throughput = total_messages as f64 / pub_time;

    (aggregate_pubstats, throughput)
}

pub(crate) fn options(config: Arc<RunnerConfig>, id: &str) -> io::Result<MqttOptions> {
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
