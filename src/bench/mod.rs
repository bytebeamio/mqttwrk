use crate::BenchConfig;
use futures::StreamExt;
use std::sync::Arc;
use tokio::task;

mod connection;

use connection::Connection;

pub(crate) async fn start(config: BenchConfig) {
    let config = Arc::new(config);
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
        let config = config.clone();
        let id = format!("rumqtt-{:05}", i);

        handles.push(task::spawn(async move {
            let mut connection = Connection::new(id, config).await.unwrap();
            connection.start().await;
        }));
    }

    loop {
        if handles.next().await.is_none() {
            break;
        }
    }
}
