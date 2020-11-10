use crate::BenchConfig;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::task;
use tokio::time;


mod connection;
mod sink;
use hdrhistogram::Histogram;

use crate::link::{Link, Status};
use connection::Connection;
use sink::Sink;
use indicatif::{ProgressBar, ProgressStyle};


pub(crate) async fn start(config: BenchConfig) {
    let config = Arc::new(config);
    let barriers_count = config.connections + config.sink;
    let barrier = Arc::new(Barrier::new(barriers_count));
    let mut handles = futures::stream::FuturesUnordered::new();
    let total_expected = config.count * config.publishers * config.connections;
    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

    let (c_tx, c_rx) = async_channel::unbounded::<Status>();
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
            c_tx.clone(),
        )
        .unwrap();

        handles.push(task::spawn(async move {
            let mut connection = Connection::new(link, config).await.unwrap();
            connection.start(barrier).await;
        }));
    }

    let (cc_tx, _) = async_channel::unbounded::<Status>();
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
            cc_tx.clone(),
        )
        .unwrap();

        handles.push(task::spawn(async move {
            let mut sink = Sink::new(link, config).await.unwrap();
            sink.start(barrier).await;
        }));
    }

    let pb = ProgressBar::new(total_expected as u64);
    pb.set_style(sty.clone());
    let mut r_cnt = 0;
    let mut cnt = 0;
    let mut hist = Histogram::<u64>::new(4).unwrap();
    

    loop {
        if cnt == config.connections || r_cnt == total_expected{
            break;
        }
    
        match c_rx.recv().await {
            Ok(status) => {
                match status{
                    Status::Hist(status) => {
                        hist.add(status).unwrap();
                        cnt += 1;
                    },
                    Status::Increment(status)=>{
                        let incr = status as usize;
                        pb.inc(status as u64);
                        r_cnt += incr;
                    },
                }
            },
            Err(e) => {},
        };
    }

    println!("Aggregate");
    println!(
        "99.999'th percentile  : {}",
        hist.value_at_quantile(0.999999)
    );
    println!(
        "99.99'th percentile   : {}",
        hist.value_at_quantile(0.99999)
    );
    println!("90 percentile         : {}", hist.value_at_quantile(0.90));
    println!("50 percentile         : {}", hist.value_at_quantile(0.5));
}
