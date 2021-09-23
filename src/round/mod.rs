use anyhow::{anyhow, Result};
use futures::{
    future::{ready, try_join_all},
    FutureExt,
};
use log::debug;
use rumqttc::{AsyncClient, ConnectionError, Event, Incoming, MqttOptions, QoS, StateError};
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::{select, sync::Barrier, task, time};
use tokio_util::sync::CancellationToken;

use crate::RoundConfig;

pub(crate) async fn start(opt: RoundConfig) -> Result<()> {
    let connections = vec![1usize, 2, 5, 10, 15, 20, 30, 40, 50, 75, 100, 150, 200];

    for (iteration, connections) in connections.iter().enumerate() {
        if iteration != 0 {
            // Cool down between each iteration
            time::sleep(time::Duration::from_secs(2)).await;
        }

        // Barrier to synchronize all connections after connect and subscribe
        let barrier = Arc::new(Barrier::new(*connections + 1));
        // Stop token to stop the connections
        let stop = CancellationToken::new();

        // Start connections
        let mut tasks = Vec::new();
        for c in 0..*connections {
            let task =
                task::spawn(connection(c, opt.clone(), stop.clone(), barrier.clone())).then(|r| {
                    match r {
                        Ok(r) => ready(r),
                        Err(e) => ready(Err(e.into())),
                    }
                });
            tasks.push(task);
        }

        // Wait until all connections are subscribed
        barrier.wait().await;

        // Wait for the test duration
        time::sleep(time::Duration::from_secs(opt.duration)).await;

        // Stop and shutdown the connections
        stop.cancel();

        // Wait for connection tasks to finish
        let result = try_join_all(tasks).await?;

        let total = result.iter().sum::<u32>();

        println!(
            "Connections: {}, Per connection avg: {}/s, total: {}/s",
            connections,
            total / *connections as u32,
            total
        );
    }

    Ok(())
}

async fn connection(
    n: usize,
    opt: RoundConfig,
    stop: CancellationToken,
    barrier: Arc<Barrier>,
) -> Result<u32> {
    debug!("[{}]: Starting", n);

    let mut mqttoptions = MqttOptions::new(uuid::Uuid::new_v4().to_string(), opt.broker, opt.port);
    mqttoptions.set_clean_session(true);
    mqttoptions.set_inflight(opt.in_flight as u16);
    mqttoptions.set_keep_alive(120);
    mqttoptions.set_request_channel_capacity(opt.in_flight + 10);

    // Initialize the client with a request queue size that is bigger than the in flight number
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, opt.in_flight + 10);

    // Each connection uses it's own random topic
    let topic = uuid::Uuid::new_v4().to_string();

    // Count publications sent
    let mut publications_sent = 0u64;

    // Count publications received
    let mut publications_received = 0u64;

    // Start timestamp
    let mut start = time::Instant::now();

    // Publication data
    let mut data = bytes::BytesMut::new();
    data.extend(std::iter::repeat(0u8).take(opt.payload_size));
    let data = data.freeze();

    'outer: loop {
        select! {
            event = eventloop.poll() => {
                match event {
                    Ok(Event::Incoming(p)) => {
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

                                // Update the start timestamp to not include the time for connectiong and subscribing
                                start = time::Instant::now();
                                client.publish_bytes(topic.clone(), QoS::AtLeastOnce, false, data.clone()).await?;
                            }
                            rumqttc::Packet::PubAck(_) if stop.is_cancelled() => {
                                // Wait until we received each publication we sent
                                if publications_received == publications_sent {
                                    // Calculate the rate in pub + res in per s
                                    let micros = time::Instant::now().duration_since(start).as_micros() + 1;
                                    let rate = (publications_received as u128 * 1000_000) / micros;
                                    debug!("[{}]: Complete. Disconnecting", n);

                                    // Disconnect the client and exit
                                    client.disconnect().await?;
                                    loop {
                                        match eventloop.poll().await {
                                            // Disconnected
                                            Ok(Event::Incoming(rumqttc::Packet::Disconnect)) => break 'outer Ok(rate as u32),
                                            // Something else
                                            Ok(_) => continue,
                                            // Disconnected
                                            Err(ConnectionError::MqttState(StateError::Io(e))) if e.kind() == ErrorKind::ConnectionAborted => break 'outer Ok(rate as u32),
                                            // Error on the connection
                                            Err(e) => break 'outer Err(e.into()),
                                        }
                                    }
                                }
                            }
                            rumqttc::Packet::PubAck(_) => {
                                debug!("[{}]: Incoming publish {:?}", n, p);
                                publications_received += 1;

                                // dbg!(publications_sent);
                                client.publish(&topic, QoS::AtLeastOnce, false, vec![0; 100]).await?;
                                // Not yet finished. Increment the publications sent counter and publish again
                                publications_sent += 1;
                            }
                            rumqttc::Packet::Disconnect => {
                                // This is an error. The broker sent us a disconnect message.
                                debug!("[{}]: Disconnected", n);
                                break Err(anyhow!("Disconnected"));
                            }
                            v => debug!("Incoming = {:?}", v)
                        }
                    }
                    Ok(Event::Outgoing(_)) => {}
                    Err(e) => break Err(e.into()),
                }
            }
            else => continue,
        }
    }
}
