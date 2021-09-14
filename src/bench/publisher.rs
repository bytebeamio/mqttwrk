use std::{sync::Arc, time::Instant};

use hdrhistogram::Histogram;
use rumqttc::{AsyncClient, EventLoop, Event, Incoming, Outgoing, QoS};
use tokio::{task, time::{self, Duration}};

use crate::{
    bench::{get_qos, options, ConnectionError},
    BenchConfig,
};

pub struct Publisher {
    id: String,
    config: Arc<BenchConfig>,
    client: AsyncClient,
    eventloop: EventLoop,
}

impl Publisher {
    pub(crate) async fn new(id: String, config: Arc<BenchConfig>) -> Result<Publisher, ConnectionError> {
        let (client, mut eventloop) = AsyncClient::new(options(config.clone(), &id)?, 10);

        loop {
            let event = eventloop.poll().await?;
            if let Event::Incoming(v) = event {
                match v {
                    Incoming::ConnAck(_) => break,
                    incoming => return Err(ConnectionError::WrongPacket(incoming)),
                }
            }
        }

        Ok(Publisher {
            id,
            config,
            client,
            eventloop,
        })
    }

    pub async fn start(&mut self) {
        let qos = get_qos(self.config.publish_qos);
        let inflight = self.config.max_inflight;
        let payload_size = self.config.payload_size;
        let count = self.config.count;
        let delay = self.config.delay;
        let id = self.id.clone();

        let start = Instant::now();
        let mut acks_expected = count;
        let mut outgoing_elapsed = Duration::from_secs(0);
        let mut acks_count = 0;

        let topic = format!("hello/{}/world", self.id);
        let client = self.client.clone();

        // If publish count is 0, don't publish. This is an idle connection
        // which can be used to test pings
        if count != 0 {
            task::spawn(async move {
                requests(topic, payload_size, count, client, qos, delay).await;
            });
        } else {
            // Just keep this connection alive
            acks_expected = 1;
        }

        if self.config.publish_qos == 0 {
            // only last extra publish is qos 1 for synchronization
            acks_expected = 1;
        }

        let mut reconnects: i32 = 0;
        let mut latencies: Vec<Option<Instant>> = vec![None; inflight as usize + 1];
        let mut histogram = Histogram::<u64>::new(4).unwrap();

        loop {
            let event = match self.eventloop.poll().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Id = {}, Connection error = {:?}", self.id, e);
                    reconnects += 1;
                    if reconnects >= 1 {
                        break;
                    }

                    continue;
                }
            };

            debug!("Id = {}, {:?}, count {}", self.id, event, acks_count);
            match event {
                Event::Incoming(v) => match v {
                    Incoming::PubAck(ack) => {
                        acks_count += 1;
                        let elapsed = match latencies[ack.pkid as usize] {
                            Some(instant) => instant.elapsed(),
                            None => {
                                warn!("Id = {}, Unsolicited PubAck", ack.pkid);
                                continue;
                            }
                        };
                        histogram.record(elapsed.as_millis() as u64).unwrap();
                    }
                    Incoming::PingResp => {}
                    incoming => {
                        error!("Id = {}, Unexpected incoming packet = {:?}", id, incoming);
                        break;
                    }
                },
                Event::Outgoing(Outgoing::Publish(pkid)) => {
                    latencies[pkid as usize] = Some(Instant::now());
                }
                _ => (),
            }

            if acks_count >= acks_expected {
                outgoing_elapsed = start.elapsed();
                break;
            }
        }

        let outgoing_throughput = (count * 1000) as f32 / outgoing_elapsed.as_millis() as f32;

        println!(
            "Id = {}
            Throughputs
            ----------------------------
            Outgoing publishes : Received = {:<7} Throughput = {} messages/s
            Reconnects         : {}

            Latencies of {} samples
            ----------------------------
            100                 : {}
            99.9999 percentile  : {}
            99.999 percentile   : {}
            90 percentile       : {}
            50 percentile       : {}
            ",
            self.id,
            acks_count,
            outgoing_throughput,
            reconnects,
            histogram.len(),
            histogram.value_at_percentile(100.0),
            histogram.value_at_percentile(99.9999),
            histogram.value_at_percentile(99.999),
            histogram.value_at_percentile(90.0),
            histogram.value_at_percentile(50.0),
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

    for i in 0..count {
        let payload = vec![0; payload_size];
        if let Some(interval) = &mut interval {
            interval.tick().await;
        }

        // These errors are usually due to eventloop task being dead. We can ignore the
        // error here as the failed eventloop task would have already printed an error
        if let Err(_e) = client.publish(topic.as_str(), qos, false, payload).await {
            break;
        }

        info!("published {}", i);
    }

    if qos == QoS::AtMostOnce {
        let payload = vec![0; payload_size];
        if let Err(_e) = client.publish(topic.as_str(), QoS::AtLeastOnce, false, payload).await {
            return;
        }
    }
}
