use std::io;
use std::sync::Arc;
use std::time::Instant;

use crate::BenchConfig;

use rumqttc::*;
use thiserror::Error;
use tokio::sync::Barrier;
use tokio::time::Duration;
use tokio::{pin, select, task, time};

pub(crate) struct Connection {
    id: String,
    config: Arc<BenchConfig>,
    client: AsyncClient,
    eventloop: EventLoop
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("IO error = {0:?}")]
    Io(#[from] io::Error),
    #[error("Connection error = {0:?}")]
    Connection(#[from] rumqttc::ConnectionError),
    #[error("Wrong packet = {0:?}")]
    WrongPacket(Incoming),
}

impl Connection {
    pub async fn new(
        id: String,
        config: Arc<BenchConfig>,
    ) -> Result<Connection, ConnectionError> {
        let subscribers = config.subscribers;
        let publishers = config.publishers;
        let connections = config.connections;
        let qos = config.qos;

        let (client, mut eventloop) = AsyncClient::new(config.options(&id)?, 10);
        let subscriber_client = client.clone();

        // If there are 10 connections with 3 publishers and 2 subscribers,
        // each connection publishes to topics 'hello/rumqtt-{id}/{0, 1, 2}/world'
        // and subscribes to all the topics of first 2 connections. 'hello/rumqtt-{0, 1}/+/world'
        // 2 subscribers implies, every connection subscribes to all the topics
        // for first 2 connections
        task::spawn(async move {
            if subscribers > 0 {
                assert!(publishers > 0, "There should be at least 1 publisher");
                assert!(
                    connections >= subscribers,
                    "#connections should be > subscribers"
                );
            }

            for i in 0..subscribers {
                let topic = format!("hello/rumqtt-{:05}/+/world", i);
                subscriber_client.subscribe(topic, get_qos(qos)).await.unwrap();
            }
        });

        // Handle connection and subscriptions first
        let mut sub_ack_count = 0;
        loop {
            let event = eventloop.poll().await?;
            if let Event::Incoming(v) = event {
                match v {
                    Incoming::SubAck(_) => sub_ack_count += 1,
                    Incoming::ConnAck(_) => (),
                    incoming => return Err(ConnectionError::WrongPacket(incoming)),
                }
            }

            if sub_ack_count >= subscribers {
                break;
            }
        }

        Ok(Connection { id, config, client, eventloop })
    }

    pub async fn start(&mut self, barrier: Arc<Barrier>) {
        let start = Instant::now();

        // Wait for all the subscription from other connections to finish
        // while doing ping requests so that broker doesn't disconnect
        let barrier = barrier.wait();
        pin!(barrier);

        // println!("await barrier = {:?}", self.id);
        loop {
            select! {
                _ = self.eventloop.poll() => {},
                _ = &mut barrier => break,
            }
        }

        // println!("done barrier = {:?}", self.id);
        if self.id == "rumqtt-00000" {
            println!(
                "Connections & subscriptions ok. Elapsed = {:?}",
                start.elapsed().as_secs()
            );
        }

        let qos = get_qos(self.config.qos);
        let payload_size = self.config.payload_size;
        let count = self.config.count;
        let publishers = self.config.publishers;
        let subscribers = self.config.subscribers;
        let delay = self.config.delay;
        let id = self.id.clone();

        let start = Instant::now();
        let acks_expected = count * publishers;
        let incoming_expected = count * publishers * subscribers;
        let mut outgoing_elapsed = Duration::from_secs(0);
        let mut incoming_elapsed = Duration::from_secs(0);
        let mut outgoing_done = false;
        let mut incoming_done = false;
        let mut acks_count = 0;
        let mut incoming_count = 0;

        for i in 0..publishers {
            let topic = format!("hello/{}/{}/world", self.id, i);
            let client = self.client.clone();
            task::spawn(async move {
                requests(topic, payload_size, count, client, qos, delay).await;
            });
        }

        let mut reconnects: i32 = 0;
        loop {
            let event = match self.eventloop.poll().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Id = {}, Connection error = {:?}", self.id, e);
                    reconnects += 1;
                    if reconnects == 1 {
                        break;
                    }

                    continue;
                }
            };

            // Never exit during idle connection tests
            if self.config.publishers == 0 || self.config.count == 0 {
                continue;
            }

            // println!("Id = {}, {:?}", id, incoming);

            if let Event::Incoming(v) = event {
                match v {
                    Incoming::PubAck(_pkid) => acks_count += 1,
                    Incoming::Publish(_publish) => incoming_count += 1,
                    Incoming::PingResp => {}
                    incoming => {
                        error!("Id = {}, Unexpected incoming packet = {:?}", id, incoming);
                        break;
                    }
                }
            }

            if !outgoing_done && acks_count >= acks_expected {
                outgoing_elapsed = start.elapsed();
                outgoing_done = true;
            }

            if !incoming_done && incoming_count >= incoming_expected {
                incoming_elapsed = start.elapsed();
                incoming_done = true;
            }

            if outgoing_done && incoming_done {
                break;
            }
        }

        let outgoing_throughput = (acks_count * 1000) as f32 / outgoing_elapsed.as_millis() as f32;
        let incoming_throughput =
            (incoming_count * 1000) as f32 / incoming_elapsed.as_millis() as f32;

        println!(
            "Id = {}
            Outgoing publishes : Received = {:<7} Throughput = {} messages/s
            Incoming publishes : Received = {:<7} Throughput = {} messages/s
            Reconnects         : {}",
            self.id,
            acks_count,
            outgoing_throughput,
            incoming_count,
            incoming_throughput,
            reconnects,
        );
    }
}

/// make count number of requests at specified QoS.
async fn requests(
    topic: String,
    payload_size: usize,
    count: usize,
    client: AsyncClient,
    qos: QoS,
    delay: u64,
) {
    let mut interval = match delay {
        0 => None,
        delay => Some(time::interval(time::Duration::from_secs(delay))),
    };

    for _i in 0..count {
        let payload = vec![0; payload_size];
        if let Some(interval) = &mut interval {
            interval.tick().await;
        }

        // These errors are usually due to eventloop task being dead. We can ignore the
        // error here as the failed eventloop task would have already printed an error
        if let Err(_e) = client.publish(topic.clone(), qos, false, payload).await {
            break;
        }
    }
}

/// get QoS level. Default is AtLeastOnce.
fn get_qos(qos: i16) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}
