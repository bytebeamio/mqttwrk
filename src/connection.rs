use rumqttc::{MqttOptions, EventLoop, Request, QoS, Incoming, Subscribe, PublishRaw};
use std::time::Instant;
use std::fs;

use tokio::{task, time};
use async_channel::Sender;

use crate::Config;
use std::sync::Arc;
use tokio::time::Duration;

pub(crate) struct Connection {
    id: String,
    config: Arc<Config>,
    mqttoptions: Option<MqttOptions>,
}

impl Connection {
    pub(crate) fn new(id: String, config: Arc<Config>) -> Connection {
        let mut mqttoptions = MqttOptions::new(&id, &config.server, config.port);
        mqttoptions.set_keep_alive(config.keep_alive);
        mqttoptions.set_inflight(config.max_inflight);
        mqttoptions.set_conn_timeout(config.conn_timeout);
        mqttoptions.set_max_request_batch(10);

        if let Some(ca_file) = &config.ca_file {
            let ca = fs::read(ca_file).unwrap();
            mqttoptions.set_ca(ca);
        }

        if let Some(client_cert_file) = &config.client_cert {
            let cert = fs::read(client_cert_file).unwrap();
            let key = fs::read(config.client_key.as_ref().unwrap()).unwrap();
            mqttoptions.set_client_auth(cert, key);
        }

        Connection {
            id,
            config,
            mqttoptions: Some(mqttoptions),
        }
    }

    pub async fn start(&mut self) {
        let mqttoptions = self.mqttoptions.take().unwrap();
        let mut eventloop = EventLoop::new(mqttoptions, 10).await;
        let requests_tx = eventloop.handle();

        let qos = get_qos(self.config.qos);
        let payload_size = self.config.payload_size;
        let count = self.config.count;
        let publishers = self.config.publishers;
        let subscribers = self.config.subscribers;
        let delay = self.config.delay;

        task::spawn(async move {
            // subscribes
            for i in 0..subscribers {
                let topic = format!("hello/{}/world", i);
                let rx = requests_tx.clone();
                subscribe(topic, rx, qos).await;
            }

            time::delay_for(Duration::from_secs(1)).await;

            for i in 0..publishers {
                let topic = format!("hello/{}/world", i);
                let rx = requests_tx.clone();
                task::spawn(async move {
                    requests(topic, payload_size, count, rx, qos, delay).await;
                });
            }
        });

        let start = Instant::now();
        let mut acks_count = 0;
        let mut incoming_count = 0;
        let acks_expected = self.config.count * self.config.publishers;
        let incoming_expected = self.config.count * self.config.publishers * self.config.subscribers;
        let mut outgoing_elapsed = Duration::from_secs(0);
        let mut incoming_elapsed = Duration::from_secs(0);
        let mut outgoing_done = false;
        let mut incoming_done = false;

        let mut reconnects: i32 = 0;
        loop {
            let (incoming, _outgoing) = match eventloop.poll().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Id = {}, Connection error = {:?}", self.id, e);
                    reconnects += 1;
                    if reconnects == 10 {
                        break;
                    }

                    continue;
                }
            };

            match incoming {
                Some(v) => {
                    match v {
                        Incoming::PubAck(_pkid) => {
                            acks_count += 1;
                        }
                        Incoming::SubAck(_suback) => {
                            continue
                        }
                        Incoming::Publish(_publish) => {
                            incoming_count += 1;
                        }
                        _ => {
                            continue;
                        }
                    }
                }
                None => {}
            }

            if acks_count >= acks_expected {
                outgoing_elapsed = start.elapsed();
                outgoing_done = true;
            }

            if incoming_count >= incoming_expected  {
                incoming_elapsed = start.elapsed();
                incoming_done = true;
            }

            if incoming_done && outgoing_done {
                break
            }
        }

        let incoming_throughput = (incoming_count * 1000) as f32 / incoming_elapsed.as_millis() as f32;
        let outgoing_throughput = (acks_count * 1000) as f32 / outgoing_elapsed.as_millis() as f32;

        println!(
            "Id = {},
            Outgoing publishes : Received = {:<7} Throughput = {} messages/s,
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
async fn requests(topic: String, payload_size: usize, count: usize, requests_tx: Sender<Request>, qos: QoS, delay: u64) {
    match delay {
        0 => {
            for _i in 0..count {
                let publish = create_publish(&topic, payload_size, qos);
                requests_tx.send(publish).await.unwrap();
            }
        },
        _ => {
            let mut interval = time::interval(time::Duration::from_secs(delay));
            for _i in 0..count {
                let publish = create_publish(&topic, payload_size, qos);
                requests_tx.send(publish).await.unwrap();
                interval.tick().await;
                
            }
        },
    };
}

/// create Request
fn create_publish(topic: &str, payload_size: usize, qos:QoS) -> Request {
    let payload = vec![0; payload_size];
    let publish = PublishRaw::new(topic, qos, payload).unwrap();
    let publish = Request::PublishRaw(publish);
    publish
}

/// create subscriptions for a topic.
async fn subscribe(topic: String, requests_tx: Sender<Request>, qos: QoS) {
    let subscription = Subscribe::new(&topic, qos);
    requests_tx.send(Request::Subscribe(subscription)).await.unwrap();
}

/// get QoS level. Default is AtLeastOnce.
fn get_qos(qos: i16) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce
    }
}

