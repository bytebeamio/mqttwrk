use rumqttc::{MqttOptions, EventLoop, Request, QoS, Incoming, Publish, Subscribe, PublishRaw};
use std::time::Instant;
use std::fs;

use tokio::task;
use async_channel::Sender;

use crate::Config;
use std::sync::Arc;

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

        task::spawn(async move {
            // subscribes
            for i in 0..subscribers {
                let topic = format!("hello/{}/world", i);
                let rx = requests_tx.clone();
                subscribe(topic, rx, qos).await;
            }

            for i in 0..publishers {
                let topic = format!("hello/{}/world", i);
                let rx = requests_tx.clone();
                task::spawn(async move {
                    requests(topic, payload_size, count, rx, qos).await;
                });
            }
        });




        let start = Instant::now();
        let mut acks_count = 0;
        let mut incoming_count = 0;
        let incoming_expected = self.config.count * self.config.publishers * self.config.subscribers;
        let acks_expected = self.config.count * self.config.publishers;

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

            if incoming_count >= incoming_expected && acks_count >= acks_expected {
                break;
            }
        }

        let total_incoming_size = self.config.payload_size * incoming_count as usize;
        let incoming_throughput = incoming_count * 1000 / start.elapsed().as_millis() as usize;

        let total_outgoing_size = self.config.payload_size * acks_count as usize;
        let acks_throughput = acks_count * 1000 / start.elapsed().as_millis() as usize;

        println!(
            "Id = {},
            Acks       : Received = {:<9}, Received size = {:<9}, Incoming Throughput = {} messages/s,
            Incoming   : Received = {:<9}, Received size = {:<9}, Incoming Throughput = {} messages/s
            Reconnects : {}",
            self.id,
            acks_count,
            total_outgoing_size,
            acks_throughput,
            incoming_count,
            total_incoming_size,
            incoming_throughput,
            reconnects,
        );
    }
}


/// make count number of requests at specified QoS.
async fn requests(topic: String, payload_size: usize, count: usize, requests_tx: Sender<Request>, qos: QoS) {
    let payloads = generate_payloads(count, payload_size);
    for payload in payloads {
        // let mut payload = vec![0; payload_size];
        // payload[0] = (i % 255) as u8;
        // let publish = Publish::new(&topic, qos, payload);
        // let publish = Request::Publish(publish);

        let publish = PublishRaw::new(&topic, qos, payload).unwrap();
        let publish = Request::PublishRaw(publish);
        requests_tx.send(publish).await.unwrap();
    }
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

fn generate_payloads(count: usize, payload_size: usize) -> Vec<Vec<u8>> {
    vec![vec![1; payload_size]; count]
}
