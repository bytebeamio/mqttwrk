use crate::BenchConfig;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::task;

mod connection;
mod sink;

use crate::link::Link;
use connection::Connection;
use sink::Sink;

pub(crate) async fn start(config: BenchConfig) {
    let config = Arc::new(config);
    let barriers_count = config.connections + config.sink;
    let barrier = Arc::new(Barrier::new(barriers_count));
    let mut handles = futures::stream::FuturesUnordered::new();

    // * Spawning too many connections wouldn't lead to `Elapsed` error
    //   in last spawns due to broker accepting connections sequentially
    // * We have to synchronize all subscription with a barrier because
    //   subscriptions shouldn't happen after publish to prevent wrong
    //   incoming publish count
    //
    // But the problem which doing connection synchronously (next connection
    // happens only after current connack is received) is that remote connections
    // will take a long time to establish 10K connection (much greater than#[str]
    // 10K * 1 millisecond)
    for i in 0..config.connections {
        let barrier = barrier.clone();
        let config = config.clone();

        let id = format!("rumqtt-{:05}", i);
        let link = Link::new(
            &id,
            &config.server,
            config.port,
            config.keep_alive,
            config.max_inflight,
            config.conn_timeout,
            config.ca_file.clone(),
            config.client_cert.clone(),
            config.client_key.clone(),
        )
        .unwrap();

        handles.push(task::spawn(async move {
            let mut connection = Connection::new(link, config).await.unwrap();
            connection.start(barrier).await;
        }));
    }

    for i in 0..config.sink {
        let barrier = barrier.clone();
        let config = config.clone();

        let id = format!("rumqtt-sink-{:05}", i);
        let link = Link::new(
            &id,
            &config.server,
            config.port,
            config.keep_alive,
            config.max_inflight,
            config.conn_timeout,
            config.ca_file.clone(),
            config.client_cert.clone(),
            config.client_key.clone(),
        )
        .unwrap();

        handles.push(task::spawn(async move {
            let mut sink = Sink::new(link, config).await.unwrap();
            sink.start(barrier).await;
        }));
    }

    loop {
        if handles.next().await.is_none() {
            break;
        }
    }
}
