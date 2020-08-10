use rumqttc::{MqttOptions, EventLoop, Request, QoS, Incoming};
use std::time::{Duration, Instant};
use std::collections::HashSet;
use std::fs;

use rand::Rng;
use tokio::task;
use tokio::time;
use async_channel::Sender;

use crate::{Metrics, Config};
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


        // subscribes
        for i in 0..self.config.subscribers {
            let topic = format!("hello/{}/world", i);
            let qos = get_qos(self.config.qos);
            let rx = requests_tx.clone();
            task::spawn(async move {
                subscribe(topic, rx, qos).await;
            });
        }

        for i in 0..self.config.publishers {
            let topic = format!("hello/{}/world", i);
            let payload_size = self.config.payload_size;
            let count = self.config.count;
            let rx = requests_tx.clone();
            let qos = get_qos(self.config.qos);
            task::spawn(async move {
                requests(topic, payload_size, count, rx, qos).await;
            });
        }

        let mut acks = acklist(self.config.count);
        let mut incoming = acklist(self.config.count);
        let mut data = Metrics {
            progress: 0,
        };

        let start = Instant::now();
        let mut acks_elapsed_ms = 0;
        let mut reconnects: i32 = 0;
        let mut reconnect_threshold: i32 = 0;

        loop {
            let res = match eventloop.poll().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Id = {}, Connection error = {:?}", self.id, e);
                    reconnects = reconnects + 1;
                    reconnect_threshold += 1;
                    // 100 continous reconnect, break
                    if reconnect_threshold == 100 {
                        break;
                    }
                    continue;
                }
            };
            let (inc, _ouc) = res;
            reconnect_threshold = 0;
            match inc {
                Some(v) => {
                    match v {
                        Incoming::PubAck(pkid) => {
                            acks.remove(&pkid.pkid);
                            acks_elapsed_ms = start.elapsed().as_millis();
                            continue;
                        }
                        Incoming::SubAck(suback) => {
                            acks.remove(&suback.pkid);
                        }
                        Incoming::Publish(publish) => {
                            data.progress = publish.pkid;
                            incoming.remove(&publish.pkid);
                        }
                        _ => {
                            continue;
                        }
                    }
                }
                None => {}
            }
            if incoming.len() == 0 {
                break;
            }
        }

        let incoming_elapsed_ms = start.elapsed().as_millis();
        let incoming_count = self.config.count - incoming.len() as u16;
        let total_incoming_size = self.config.payload_size * incoming_count as usize;
        let incoming_throughput = total_incoming_size / incoming_elapsed_ms as usize;
        let incoming_throughput_mbps = incoming_throughput * 1000 / 1024;

        let acks_count = self.config.count - acks.len() as u16;
        let total_outgoing_size = self.config.payload_size * acks_count as usize;
        let acks_throughput = total_outgoing_size / acks_elapsed_ms as usize;
        let acks_throughput_mbps = acks_throughput * 1000 / 1024;

        println!(
            "Id = {},
            Acks:     Missed = {:<5}, Received size = {}, Incoming Throughput = {} KB/s,
            Incoming: Missed = {:<5}, Received size = {}, Incoming Throughput = {} KB/s
            Reconnects: {}",
            self.id,
            acks.len(),
            total_outgoing_size,
            acks_throughput_mbps,
            incoming.len(),
            total_incoming_size,
            incoming_throughput_mbps,
            reconnects,
        );
    }
}


/// make count number of requests at specified QoS.
async fn requests(topic: String, payload_size: usize, count: u16, requests_tx: Sender<Request>, qos: QoS) {
    for i in 0..count {
        let mut payload = generate_payload(payload_size);
        payload[0] = (i % 255) as u8;
        let publish = rumqttc::Publish::new(&topic, qos, payload);
        let publish = Request::Publish(publish);
        if let Err(_) = requests_tx.send(publish).await {
            break;
        }
    }
    time::delay_for(Duration::from_secs(5)).await;
}

/// create subscriptions for a topic.
async fn subscribe(topic: String, requests_tx: Sender<Request>, qos: QoS) {
    let subscription = rumqttc::Subscribe::new(&topic, qos);
    requests_tx.send(Request::Subscribe(subscription)).await.unwrap();
}

/// create acklist
fn acklist(count: u16) -> HashSet<u16> {
    let mut acks = HashSet::new();
    for i in 1..=count {
        acks.insert(i);
    }

    acks
}

/// generate payload of sepcified byte size.
fn generate_payload(payload_size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let payload: Vec<u8> = (0..payload_size).map(|_| rng.gen_range(0, 255)).collect();
    payload
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
