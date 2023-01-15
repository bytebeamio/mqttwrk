use std::{
    fs, io,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use fake::{Dummy, Fake, Faker};
use hdrhistogram::Histogram;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, Outgoing, QoS, Transport};
use serde::Serialize;
use tokio::{
    sync::Barrier,
    task,
    time::{self, Duration},
};

use crate::{bench::ConnectionError, simulator::PubStats, SimulatorConfig};

#[derive(Debug, Serialize, Dummy)]
struct Imu {
    sequence: u32,
    timestamp: u64,
    ax: f64,
    ay: f64,
    az: f64,
    pitch: f64,
    roll: f64,
    yaw: f64,
    magx: f64,
    magy: f64,
    magz: f64,
}

pub struct Publisher {
    id: String,
    config: Arc<SimulatorConfig>,
    client: AsyncClient,
    eventloop: EventLoop,
}

impl Publisher {
    pub(crate) async fn new(
        id: String,
        config: Arc<SimulatorConfig>,
    ) -> Result<Publisher, ConnectionError> {
        let (client, mut eventloop) = AsyncClient::new(options(config.clone(), &id)?, 10);
        eventloop.network_options.set_connection_timeout(10);

        loop {
            let event = match eventloop.poll().await {
                Ok(v) => v,
                Err(rumqttc::ConnectionError::NetworkTimeout)
                | Err(rumqttc::ConnectionError::FlushTimeout) => {
                    println!("{} reconnecting", id);
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            if let Event::Incoming(v) = event {
                match v {
                    Incoming::ConnAck(_) => {
                        println!("{} connected", id);
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
        })
    }

    pub async fn start(&mut self, barrier_handle: Arc<Barrier>) -> PubStats {
        let qos = get_qos(self.config.publish_qos);
        let inflight = self.config.max_inflight;
        let count = self.config.count;
        let rate = self.config.rate_pub;
        let id = self.id.clone();

        let start = Instant::now();
        let mut acks_expected = count;
        let mut outgoing_elapsed = Duration::from_secs(0);
        let mut acks_count = 0;

        let topic = self.config.topic_format.replace("{}", &self.id);
        let client = self.client.clone();

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

        // If publish count is 0, don't publish. This is an idle connection
        // which can be used to test pings
        if count != 0 {
            // delay between messages in milliseconds
            let delay = if rate == 0 { 0 } else { 1000 / rate };
            task::spawn(async move {
                requests(topic, count, client, qos, delay).await;
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

        // println!(
        //     "Id = {}
        //     Throughputs
        //     ----------------------------
        //     Outgoing publishes : {:<7} Throughput = {} messages/s
        //     Reconnects         : {}
        //
        //     Latencies of {} samples
        //     ----------------------------
        //     100                 : {}
        //     99.9999 percentile  : {}
        //     99.999 percentile   : {}
        //     90 percentile       : {}
        //     50 percentile       : {}
        //     ",
        //     self.id,
        //     acks_count,
        //     outgoing_throughput,
        //     reconnects,
        //     histogram.len(),
        //     histogram.value_at_percentile(100.0),
        //     histogram.value_at_percentile(99.9999),
        //     histogram.value_at_percentile(99.999),
        //     histogram.value_at_percentile(90.0),
        //     histogram.value_at_percentile(50.0),
        // );

        PubStats {
            outgoing_publish: acks_count as u64,
            throughput: outgoing_throughput,
            reconnects,
        }
    }
}

fn dummy_imu(sequence: u32) -> Imu {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    Imu {
        sequence,
        timestamp,
        ax: Faker.fake::<f64>(),
        ay: Faker.fake::<f64>(),
        az: Faker.fake::<f64>(),
        pitch: Faker.fake::<f64>(),
        roll: Faker.fake::<f64>(),
        yaw: Faker.fake::<f64>(),
        magx: Faker.fake::<f64>(),
        magy: Faker.fake::<f64>(),
        magz: Faker.fake::<f64>(),
    }
}

/// make count number of requests at specified QoS.
async fn requests(topic: String, count: usize, client: AsyncClient, qos: QoS, delay: u64) {
    let mut interval = match delay {
        0 => None,
        delay => Some(time::interval(time::Duration::from_millis(delay))),
    };

    for i in 0..count {
        let fake_data = vec![dummy_imu(i as u32)];
        let payload = serde_json::to_string(&fake_data).unwrap();
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
        let fake_data = vec![dummy_imu(count as u32)];
        let payload = serde_json::to_string(&fake_data).unwrap();
        if let Err(_e) = client
            .publish(topic.as_str(), QoS::AtLeastOnce, false, payload)
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

fn options(config: Arc<SimulatorConfig>, id: &str) -> io::Result<MqttOptions> {
    let mut options = MqttOptions::new(id, &config.server, config.port);
    options.set_keep_alive(Duration::from_secs(config.keep_alive));
    options.set_inflight(config.max_inflight);

    if let Some(ca_file) = &config.ca_file {
        let ca = fs::read(ca_file)?;
        options.set_transport(Transport::tls(ca, None, None));
    }

    Ok(options)
}
