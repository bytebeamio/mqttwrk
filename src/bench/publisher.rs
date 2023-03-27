use std::{collections::HashMap, fs, io, sync::Arc, time::Instant};

use crate::{
    bench::ConnectionError,
    cli::{DataEvent, RunnerConfig},
    common::{PubStats, UNIQUE_ID},
};
use futures::StreamExt;
use hdrhistogram::Histogram;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, Outgoing, QoS, Transport};
use tokio::{
    sync::Barrier,
    task,
    time::{self, Duration},
};

use super::gendata::GenData;

pub struct Publisher {
    id: String,
    config: Arc<RunnerConfig>,
    client: AsyncClient,
    eventloop: EventLoop,
    generator: GenData,
}

impl Publisher {
    pub(crate) async fn new(
        id: String,
        config: Arc<RunnerConfig>,
    ) -> Result<Publisher, ConnectionError> {
        let (client, mut eventloop) = AsyncClient::new(options(config.clone(), &id)?, 10);
        eventloop
            .network_options
            .set_connection_timeout(config.conn_timeout);

        let generator = GenData::new(config.count, config.tasks.clone());

        loop {
            let event = match eventloop.poll().await {
                Ok(v) => v,
                Err(rumqttc::ConnectionError::NetworkTimeout)
                | Err(rumqttc::ConnectionError::FlushTimeout) => {
                    println!("{id} reconnecting");
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            if let Event::Incoming(v) = event {
                match v {
                    Incoming::ConnAck(_) => {
                        // println!("{id} connected");
                        break;
                    }
                    incoming => return Err(ConnectionError::WrongPacket(incoming)),
                }
            }
        }

        Ok(Publisher {
            id,
            config,
            client,
            eventloop,
            generator,
        })
    }

    pub async fn start(mut self, barrier_handle: Arc<Barrier>) -> PubStats {
        let qos = get_qos(self.config.publish_qos);
        let inflight = self.config.max_inflight;
        let count = self.config.count;
        let id = self.id.clone();
        let generator = self.generator;

        let mut acks_expected = count;
        let mut outgoing_elapsed = Duration::from_secs(0);
        let mut acks_count = 0;

        let mut topic = self.config.topic_format.replace("{pub_id}", &self.id);
        topic = topic.replace("{unique_id}", &UNIQUE_ID);
        let client = self.client;

        let wait = barrier_handle.wait();
        tokio::pin!(wait);

        // Keep sending pings until all publishers are spawned
        loop {
            tokio::select! {
                _ = wait.as_mut() => {
                    break;
                }
                _ = self.eventloop.poll() => {
                }
            };
        }

        let start = Instant::now();

        // If publish count is 0, don't publish. This is an idle connection
        // which can be used to test pings
        if count != 0 {
            task::spawn(async move {
                requests(topic, client, qos, generator).await;
            });
        } else {
            // Just keep this connection alive
            acks_expected = 1;
        }

        if self.config.publish_qos == 0 {
            // only last extra publish is qos 1 for synchronization
            acks_expected = 1;
        }

        let mut reconnects: u64 = 0;
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
                    Incoming::PingResp => {
                        debug!("ping response")
                    }
                    incoming => {
                        error!("Id = {}, Unexpected incoming packet = {:?}", id, incoming);
                        break;
                    }
                },
                Event::Outgoing(Outgoing::Publish(pkid)) => {
                    latencies[pkid as usize] = Some(Instant::now());
                }
                Event::Outgoing(Outgoing::PingReq) => {
                    debug!("ping request")
                }
                _ => (),
            }

            if acks_count >= acks_expected {
                outgoing_elapsed = start.elapsed();
                break;
            }
        }

        let outgoing_throughput = (count * 1000) as f32 / outgoing_elapsed.as_millis() as f32;

        if self.config.show_pub_stat {
            println!(
                "Id = {}
            Throughputs
            ----------------------------
            Outgoing publishes : {:<7} Throughput = {} messages/s
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

        // if publish_qos is 0 assume we send all publishes
        if self.config.publish_qos == 0 {
            acks_count = count;
        }

        PubStats {
            outgoing_publish: acks_count as u64,
            throughput: outgoing_throughput,
            reconnects,
        }
    }
}

/// make count number of requests at specified QoS.
async fn requests(topic: String, client: AsyncClient, qos: QoS, generator: GenData) {
    let mut stream = generator.into_stream();
    let mut topic_cache: HashMap<DataEvent, String> = HashMap::new();

    while let Some((event, payload)) = stream.next().await {
        // These errors are usually due to eventloop task being dead. We can ignore the
        // error here as the failed eventloop task would have already printed an error
        topic_cache
            .entry(event)
            .or_insert(topic.replace("{data_type}", &event.to_string()));
        if let Err(_e) = client
            .publish(topic_cache.get(&event).unwrap(), qos, false, payload)
            .await
        {
            break;
        }

        info!("published {}", event.sequence());
    }

    if qos == QoS::AtMostOnce {
        if let Err(_e) = client
            .publish("test/complete", QoS::AtLeastOnce, false, "")
            .await
        {
            // TODO
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

fn options(config: Arc<RunnerConfig>, id: &str) -> io::Result<MqttOptions> {
    let mut options = MqttOptions::new(id, &config.server, config.port);
    options.set_keep_alive(Duration::from_secs(config.keep_alive));
    options.set_inflight(config.max_inflight);

    if let Some(ca_file) = &config.ca_file {
        let ca = fs::read(ca_file)?;
        options.set_transport(Transport::tls(ca, None, None));
    }

    Ok(options)
}
