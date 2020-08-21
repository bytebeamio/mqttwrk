use std::time::Instant;
use std::fs;
use std::sync::Arc;

use crate::Config;

use tokio::{task, time};
use tokio::sync::Barrier;
use tokio::time::Duration;
use async_channel::Sender;
use rumqttc::{MqttOptions, EventLoop, Request, QoS, Incoming, Subscribe, PublishRaw};


const ID_PREFIX: &str = "rumqtt";

pub(crate) struct Connection {
    id: String,
    config: Arc<Config>,
    mqttoptions: Option<MqttOptions>,
}

impl Connection {
    pub(crate) fn new(id: usize, config: Arc<Config>) -> Connection {
        let id = format!("{}-{}", ID_PREFIX, id);
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

    pub async fn start(&mut self, barrier: Arc<Barrier>) {
        let mqttoptions = self.mqttoptions.take().unwrap();
        let mut eventloop = EventLoop::new(mqttoptions, 10).await;
        let requests_tx = eventloop.handle();

        let qos = get_qos(self.config.qos);
        let payload_size = self.config.payload_size;
        let count = self.config.count;
        let publishers = self.config.publishers;
        let subscribers = self.config.subscribers;
        let delay = self.config.delay;
        let id = self.id.clone();

        task::spawn(async move {
            // subscribes
            for i in 0..subscribers {
                // Subscribe to one topic per connection
                let topic = format!("hello/{}-{}/0/world", ID_PREFIX, i);
                let rx = requests_tx.clone();
                subscribe(topic, rx, qos).await;
            }

            time::delay_for(Duration::from_secs(1)).await;

            for i in 0..publishers {
                let topic = format!("hello/{}/{}/world", id, i);
                let rx = requests_tx.clone();
                task::spawn(async move {
                    requests(topic, payload_size, count, rx, qos, delay).await;
                });
            }
        });

        // Handle connection and subscriptions first
        let mut sub_ack_count = 0;
        loop {
            let (incoming, _outgoing) = match eventloop.poll().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Id = {}, Connection error = {:?}", self.id, e);
                    return
                }
            };

            if let Some(v) = incoming {
                match v {
                    Incoming::SubAck(_suback) => sub_ack_count += 1,
                    Incoming::Connected => continue,
                    incoming => {
                        error!("Unexpected incoming packet = {:?}", incoming);
                        return
                    }     
                }
            }


            if sub_ack_count >= self.config.subscribers {
                break 
            }
        }

        // Barrier for all the subscriptions across multiple connections to complete.
        // Otherwise late connections will loose publishes because they subscribe late.
        // Leads to main loop never reaching target incoming count (hence blocked forever)
        barrier.wait().await;

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
                    if reconnects == 1 {
                        break;
                    }

                    continue;
                }
            };

            if let Some(v) = incoming {
                match v {
                   Incoming::PubAck(_pkid) => acks_count += 1,
                   Incoming::Publish(_publish) => incoming_count += 1,
                   Incoming::PingResp => continue,
                   incoming => {
                       error!("Unexpected incoming packet = {:?}", incoming);
                       break;
                   }
               }
            }

            if !outgoing_done && acks_count >= acks_expected {
                outgoing_elapsed = start.elapsed();
                outgoing_done = true;
            }
;
            if !incoming_done && incoming_count >= incoming_expected  {
                incoming_elapsed = start.elapsed();
                incoming_done = true;
            }

            if outgoing_done && incoming_done {
                break
            }
        }

        let outgoing_throughput = (acks_count * 1000) as f32 / outgoing_elapsed.as_millis() as f32;
        let incoming_throughput = (incoming_count * 1000) as f32 / incoming_elapsed.as_millis() as f32;

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
async fn requests(topic: String, payload_size: usize, count: usize, requests_tx: Sender<Request>, qos: QoS, delay: u64) {
    let mut interval = match delay {
        0 => None,
        delay => Some(time::interval(time::Duration::from_secs(delay)))
    };

    for _i in 0..count {
        let payload = vec![0; payload_size];
        // payload[0] = (i % 255) as u8;
        let publish = PublishRaw::new(&topic, qos, payload).unwrap();
        let publish = Request::PublishRaw(publish);
        if let Some(interval) = &mut interval {
            interval.tick().await;
        }

        // These errors are usually due to eventloop task being dead. We can ignore the
        // error here as the failed eventloop task would have already printed an error
        if let Err(_e) = requests_tx.send(publish).await {
            break
        }
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

