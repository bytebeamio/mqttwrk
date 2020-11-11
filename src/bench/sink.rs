use std::io;
use std::sync::Arc;
use std::time::Instant;

use crate::BenchConfig;

use crate::link::Link;
use rumqttc::*;
use thiserror::Error;
use tokio::sync::Barrier;
use tokio::time::Duration;
use tokio::{pin, select, task};

pub(crate) struct Sink {
    config: Arc<BenchConfig>,
    link: Link,
}

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("IO error = {0:?}")]
    Io(#[from] io::Error),
    #[error("Connection error = {0:?}")]
    Connection(#[from] rumqttc::ConnectionError),
    #[error("Wrong packet = {0:?}")]
    WrongPacket(Incoming),
}

impl Sink {
    pub async fn new(mut link: Link, config: Arc<BenchConfig>) -> Result<Sink, SinkError> {
        let qos = config.qos;
        let client = link.client.clone();
        task::spawn(async move {
            client.subscribe("#", get_qos(qos)).await.unwrap();
        });

        loop {
            let event = link.eventloop.poll().await?;
            if let Event::Incoming(v) = event {
                match v {
                    Incoming::SubAck(_) => break,
                    Incoming::ConnAck(_) => (),
                    incoming => return Err(SinkError::WrongPacket(incoming)),
                }
            }
        }

        Ok(Sink { config, link })
    }

    pub async fn start(&mut self, barrier: Arc<Barrier>) {
        // Wait for all the subscription from other connections to finish
        // while doing ping requests so that broker doesn't disconnect
        let barrier = barrier.wait();
        pin!(barrier);

        loop {
            select! {
                _ = self.link.eventloop.poll() => {},
                _ = &mut barrier => break,
            }
        }

        let count = self.config.count;
        let publishers = self.config.publishers;
        let subscribers = self.config.subscribers;
        let id = self.link.id.clone();

        let start = Instant::now();
        let incoming_expected = count * publishers;
        let mut incoming_elapsed = Duration::from_secs(0);
        let mut incoming_count = 0;

        let mut reconnects: i32 = 0;
        while reconnects < 1 {
            let event = match self.link.eventloop.poll().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Id = {}, Connection error = {:?}", self.link.id, e);
                    reconnects += 1;
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
                    Incoming::Publish(_publish) => incoming_count += 1,
                    Incoming::PingResp => {}
                    incoming => {
                        error!("Id = {}, Unexpected incoming packet = {:?}", id, incoming);
                        break;
                    }
                }
            }

            if incoming_count >= incoming_expected {
                incoming_elapsed = start.elapsed();
                break;
            }
        }

        let elapsed = incoming_elapsed.as_millis() as f32;
        let incoming_throughput = (incoming_count * 1000) as f32 / elapsed;

        println!(
            "Id = {}
            Incoming publishes : Received = {:<7} Throughput = {} messages/s
            Reconnects         : {}",
            self.link.id, incoming_count, incoming_throughput, reconnects,
        );
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
