use anyhow::{anyhow, bail, Result};
use futures::future::try_join_all;
use log::debug;
use rumqttc::{AsyncClient, Event, MqttOptions, QoS};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{sync::Barrier, task, time};
use tokio_util::sync::CancellationToken;

use crate::RoundConfig;

pub(crate) async fn start(opt: RoundConfig) -> Result<()> {
    let connections = vec![1usize, 2, 5, 10, 15, 20, 30, 40, 50, 75, 100, 150, 200];
    // let connections = vec![70];
    let execution_time = opt.duration as u64;

    for (iteration, connections) in connections.iter().enumerate() {
        if iteration != 0 {
            // Cool down between each iteration
            time::sleep(Duration::from_secs(5)).await;
        }

        // Barrier to synchronize all connections after connect and subscribe
        let barrier = Arc::new(Barrier::new(*connections + 1));
        // Stop token to stop the connections
        let stop = CancellationToken::new();

        // Start connections
        let mut tasks = Vec::new();
        for c in 0..*connections {
            let barrier = barrier.clone();
            let stop = stop.clone();
            let opt = opt.clone();
            let task = task::spawn(async move {
                let v = time::timeout(
                    Duration::from_secs(opt.duration as u64 + 10),
                    connection(c, opt, stop, barrier),
                )
                .await;

                let v = match v {
                    Ok(v) => v,
                    Err(e) => bail!("connection: {:<5} error: {}", c, e),
                };

                let v = match v {
                    Ok(v) => v,
                    Err(e) => bail!("connection: {:<5} error: {}", c, e),
                };

                Ok::<_, anyhow::Error>(v)
            });

            tasks.push(task);
        }

        // Start execution time count in a task. Or else, connection errors
        // won't propogate to try_join_all immediately
        task::spawn(async move {
            // Wait until all connections are subscribed before waiting for execution_time seconds
            barrier.wait().await;

            // Wait for the test duration
            time::sleep(Duration::from_secs(execution_time)).await;

            // Stop and shutdown the connections
            stop.cancel();
        });

        // Wait for connection tasks to finish
        let results = try_join_all(tasks).await?;
        let mut success: Vec<Status> = results.iter().filter_map(|v| v.as_ref().ok()).cloned().collect();
        let total: u128 = success.iter().map(|v| v.throughput).sum();
        success.sort();

        let mut sent = 0;
        let mut received = 0;
        for v in success {
            sent += v.sent; 
            received += v.received;
        }

        // println!("-------------------------------------------------------------------------------");
        // for v in success {
        //     println!(
        //         "connection: {:<5} out: {:<10} in: {:<10} miss: {:<5} throughput: {}/s",
        //         v.id,
        //         v.sent,
        //         v.received,
        //         v.sent - v.received,
        //         v.throughput
        //     )
        // }
        // println!("-------------------------------------------------------------------------------");

        // let failures: Vec<&anyhow::Error> = results.iter().filter_map(|v| v.as_ref().err()).collect();
        // for v in failures {
        //     println!("{}", v);
        // }

        println!(
            "Connections: {:3} Sent: {:10} Miss: {:5} Per connection avg: {:7}/s Total: {}/s",
            connections,
            sent,
            sent - received,
            total / *connections as u128,
            total
        );
    }

    Ok(())
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct Status {
    id: usize,
    sent: u64,
    received: u64,
    throughput: u128,
}

async fn connection(
    n: usize,
    opt: RoundConfig,
    stop: CancellationToken,
    barrier: Arc<Barrier>,
) -> Result<Status> {
    debug!("[{}]: Starting", n);

    let mut mqttoptions = MqttOptions::new(n.to_string(), opt.broker, opt.port);
    mqttoptions.set_clean_session(true);
    mqttoptions.set_inflight(opt.in_flight as u16);
    mqttoptions.set_keep_alive(opt.duration);
    mqttoptions.set_request_channel_capacity(opt.in_flight + 10);

    // Initialize the client with a request queue size that is bigger than the in flight number
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, opt.in_flight + 10);

    // Each connection uses it's own random topic
    // let topic = uuid::Uuid::new_v4().to_string();
    // let topic = "hello/1/world".to_string();
    let topic = n.to_string();

    // Count publications sent
    let mut publications_sent = 0u64;

    // Count publications received
    let mut publications_received = 0u64;

    // Start timestamp
    let mut start = Instant::now();

    // Publication data
    let mut data = bytes::BytesMut::new();
    data.extend(std::iter::repeat(0u8).take(opt.payload_size));
    let data = data.freeze();

    'outer: loop {
        match eventloop.poll().await? {
            Event::Incoming(p) => {
                match p {
                    rumqttc::Packet::ConnAck(_) => {
                        debug!("[{}]: Connected", n);
                        // We're connected. Subscribe to our topic
                        client.subscribe(topic.clone(), QoS::AtLeastOnce).await?;
                    }
                    rumqttc::Packet::SubAck(_) => {
                        // This test codes does only one subscription. Receiving the
                        // suback means we're ready to go
                        debug!("[{}]: Subscribed", n);
                        debug!("[{}]: Waiting for all other connections to be here", n);
                        barrier.wait().await;
                        debug!("[{}]: All connections here", n);

                        // Update the start timestamp to not include the time for connectiong and subscribing
                        start = Instant::now();

                        // Start the publication loop by publishing n messages
                        for _ in 0..opt.in_flight {
                            publications_sent += 1;
                            client
                                .publish_bytes(topic.clone(), QoS::AtLeastOnce, false, data.clone())
                                .await?;
                        }
                    }
                    rumqttc::Packet::Publish(_) if stop.is_cancelled() => {
                        // Calculate the rate in pub + res in per s
                        let micros = Instant::now().duration_since(start).as_micros() + 1;
                        let rate = (publications_received as u128 * 1000_000) / micros;
                        let v = Status {
                            id: n,
                            sent: publications_sent,
                            received: publications_received,
                            throughput: rate,
                        };

                        break 'outer Ok(v);
                    }
                    rumqttc::Packet::Publish(v) => {
                        debug!("[{}]: Incoming publish {:?}", n, v);
                        publications_received += 1;

                        if let Some(max_publishes) = opt.max_publishes {
                            if publications_sent >= max_publishes {
                                let micros = Instant::now().duration_since(start).as_micros() + 1;
                                let rate = (publications_received as u128 * 1000_000) / micros;

                                let v = Status {
                                    id: n,
                                    sent: publications_sent,
                                    received: publications_received,
                                    throughput: rate,
                                };

                                break 'outer Ok(v);
                            }
                        }

                        client
                            .publish(&topic, QoS::AtMostOnce, false, vec![0; 100])
                            .await?;
                        // Not yet finished. Increment the publications sent counter and publish again
                        publications_sent += 1;
                    }
                    rumqttc::Packet::Disconnect => {
                        // This is an error. The broker sent us a disconnect message.
                        debug!("[{}]: Disconnected", n);
                        break Err(anyhow!("Disconnected"));
                    }
                    v => debug!("Incoming = {:?}", v),
                }
            }
            Event::Outgoing(_) => continue,
        }
    }
}
