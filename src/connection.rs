use std::time::Instant;
use std::{fs, io};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::Config;

use tokio::{task, time, select};
use tokio::sync::Barrier;
use tokio::time::Duration;
use rumqttc::{MqttOptions, EventLoop, Request, QoS, Incoming, Subscribe, PublishRaw, Sender};
use thiserror::Error;
use hdrhistogram::Histogram;
use whoami;

const ID_PREFIX: &str = "rumqtt";

pub(crate) struct Connection {
    id: String,
    config: Arc<Config>,
    eventloop: EventLoop,
    sender: Sender::<Histogram::<u64>>,
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("IO error = {0:?}")]
    Io(#[from] io::Error),
    #[error("Connection error = {0:?}")]
    Connection(#[from] rumqttc::ConnectionError),
    #[error("Wrong packet = {0:?}")]
    WrongPacket(Incoming)
}

impl Connection {
    pub async fn new(id: usize, config: Arc<Config>, tx: Sender::<Histogram::<u64>>) -> Result<Connection, ConnectionError> {
        let id = format!("{}-{}", ID_PREFIX, id);
        let device_name = whoami::devicename();
        let con_id = format!("{}-{}", id, device_name);
        let mut mqttoptions = MqttOptions::new(&con_id, &config.server, config.port);
        mqttoptions.set_keep_alive(config.keep_alive);
        mqttoptions.set_inflight(config.max_inflight);
        mqttoptions.set_connection_timeout(config.conn_timeout);
        mqttoptions.set_max_request_batch(10);
        

        if let Some(ca_file) = &config.ca_file {
            let ca = fs::read(ca_file)?;
            mqttoptions.set_ca(ca);
        }

        if let Some(client_cert_file) = &config.client_cert {
            let cert = fs::read(client_cert_file)?;
            let key = fs::read(config.client_key.as_ref().unwrap())?;
            mqttoptions.set_client_auth(cert, key);
        }


        let mut eventloop = EventLoop::new(mqttoptions, 10).await;
        let requests_tx = eventloop.handle();

        let sconfig = config.clone();
        task::spawn(async move {
            let qos = get_qos(sconfig.qos);

            // subscribes
            for i in 0..sconfig.subscribers {
                // Subscribe to one topic per connection
                let topic = format!("hello/{}-{}/0/world", ID_PREFIX, i);
                subscribe(topic, requests_tx.clone(), qos).await;
            }
        });

        // Handle connection and subscriptions first
        let mut sub_ack_count = 0;
        loop {
            let (incoming, _outgoing) = eventloop.poll().await?;
            if let Some(v) = incoming {
                match v {
                    Incoming::SubAck(_suback) => sub_ack_count += 1,
                    Incoming::Connected => (),
                    incoming => return Err(ConnectionError::WrongPacket(incoming))
                }
            }


            if sub_ack_count >= config.subscribers {
                break
            }
        }

        Ok(Connection {
            id,
            config,
            eventloop,
            sender:tx,
        })
    }

    pub async fn start(&mut self, barrier: Arc<Barrier>) {
        // Wait for all the subscription from other connections to finish
        // while doing ping requests so that broker doesn't disconnect
        loop {
            select! {
                _ = self.eventloop.poll() => {},
                _ = barrier.wait() => break,
            }
        } 

        // Wait some time for all subscriptions in rumqttd to be successfull.
        // This is a workaround because of asynchronous publish and subscribe
        // paths in rumqttd. 
        // TODO: Make them sequential in broker and remove this
        time::delay_for(Duration::from_secs(1)).await;

        if self.id == "rumqtt-0" {
            println!("All connections and subscriptions ok");
        }


        let qos = get_qos(self.config.qos);
        let payload_size = self.config.payload_size;
        let count = self.config.count;
        let publishers = self.config.publishers;
        let delay = self.config.delay;
        let id = self.id.clone();

        let requests_tx = self.eventloop.requests_tx.clone();
        for i in 0..publishers {
            let topic = format!("hello/{}/{}/world", id, i);
            let tx = requests_tx.clone();
            task::spawn(async move {
                requests(topic, payload_size, count, tx, qos, delay).await;
            });
        }

        let start = Instant::now();
        let mut acks_count = 0;
        let mut incoming_count = 0;
        let acks_expected = self.config.count * self.config.publishers;
        let incoming_expected = self.config.count * self.config.publishers * self.config.subscribers;
        let mut outgoing_elapsed = Duration::from_secs(0);
        let mut incoming_elapsed = Duration::from_secs(0);
        let mut outgoing_done = false;
        let mut incoming_done = false;
        let mut hist = Histogram::<u64>::new(4).unwrap();

        // maps to record Publish and PubAck of messages. PKID acts as key
        let mut pkids_publish:  BTreeMap::<u16, std::time::Instant> = BTreeMap::new();
        let mut pub_acks: BTreeMap::<u16, std::time::Instant> = BTreeMap::new();

        let mut reconnects: i32 = 0;
        loop {
            let (incoming, _outgoing) = match self.eventloop.poll().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Id = {}, Connection error = {:?}", self.id, e);
                    reconnects += 1;
                    if reconnects == 1 { break }

                    continue;
                }
            };

            // Never exit during idle connection tests
            if self.config.publishers == 0 || self.config.count == 0 {
                warn!("Idle connection");
                continue
            }

            if let Some(v) = incoming {
                match v {
                   Incoming::PubAck(pkid) => {
                       acks_count += 1;
                       pub_acks.insert(pkid.pkid, Instant::now());
                   },
                   Incoming::Publish(publish) => {
                        // We are not sure that whether the packet is out of TCP queue, but this
                        // is the best we can do from application layer.
                       incoming_count += 1;
                       pkids_publish.insert(publish.pkid, Instant::now());
                   }
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

        for (pkid, ack_time)  in  pub_acks.into_iter() {
            let publish_time = pkids_publish.get(&pkid).unwrap();
            let latency = publish_time.duration_since(ack_time).as_millis();
            hist.record(latency as u64).unwrap();

        }
        println!("# of samples          : {}", hist.len());
        println!("99.999'th percentile  : {}", hist.value_at_quantile(0.999999));
        println!("99.99'th percentile   : {}", hist.value_at_quantile(0.99999));
        println!("90 percentile         : {}", hist.value_at_quantile(0.90));
        println!("50 percentile         : {}", hist.value_at_quantile(0.5));
        self.sender.send(hist).await.unwrap();
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

