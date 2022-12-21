use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, Outgoing};
use tokio::time;

use crate::{
    simulator::{get_qos, options, ConnectionError, SubStats},
    SimulatorConfig,
};

pub struct Subscriber {
    id: String,
    config: Arc<SimulatorConfig>,
    #[allow(dead_code)]
    client: AsyncClient,
    eventloop: EventLoop,
}

impl Subscriber {
    pub(crate) async fn new(
        id: String,
        config: Arc<SimulatorConfig>,
    ) -> Result<Subscriber, ConnectionError> {
        let (client, mut eventloop) = AsyncClient::new(options(config.clone(), &id)?, 10);

        // waiting for connection
        loop {
            let event = eventloop.poll().await?;
            if let Event::Incoming(v) = event {
                match v {
                    Incoming::ConnAck(_) => break,
                    incoming => return Err(ConnectionError::WrongPacket(incoming)),
                }
            }
        }

        // subscribing
        client
            .subscribe(
                config.topic_format.replace("{}", &"+"),
                get_qos(config.subscribe_qos),
            )
            .await?;

        // waiting for subscription confirmation
        loop {
            let event = eventloop.poll().await?;
            if let Event::Incoming(v) = event {
                match v {
                    Incoming::SubAck(_) => break,
                    incoming => return Err(ConnectionError::WrongPacket(incoming)),
                }
            }
        }

        Ok(Subscriber {
            id,
            config,
            client,
            eventloop,
        })
    }

    pub async fn start(&mut self) -> SubStats {
        let required_publish_count = self.config.count * self.config.publishers;
        // total number of publishes received
        let mut publish_count = 0;
        // total number of pubacks sent
        let mut puback_count = 0;
        // when the very first publish arrived
        let mut start = Instant::now();
        // when the latest publish arrived
        let mut last_publish = Instant::now();
        // to record latencies
        let mut histogram = Histogram::<u64>::new(4).unwrap();
        // number of reconnects attempted
        let mut reconnects = 0;

        // for the very first publish, to record the starting time of publishes
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

            match event {
                Event::Incoming(Incoming::Publish(_)) => {
                    publish_count += 1;
                    start = Instant::now();
                    last_publish = start;
                    break;
                }
                Event::Incoming(Incoming::PingResp) => {
                    debug!("ping response");
                }
                Event::Outgoing(Outgoing::PingReq) => {
                    debug!("ping request")
                }
                Event::Outgoing(Outgoing::PubAck(_)) => {
                    puback_count += 1;
                }
                packet => {
                    error!("Id = {}, Unexpected packet = {:?}", self.id, packet,);
                    continue;
                }
            }
        }

        let mut seq = 0;
        // for remainging publishes
        while publish_count < required_publish_count {
            let event = match self.eventloop.poll().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Id = {}, Connection error = {:?}", self.id, e);
                    reconnects += 1;
                    if reconnects >= 2 {
                        break;
                    }
                    continue;
                }
            };

            debug!("Id = {}, {:?}, count = {}", self.id, event, publish_count);

            match event {
                Event::Incoming(Incoming::Publish(_)) => {
                    seq += 1;
                    publish_count += 1;
                    histogram
                        .record(last_publish.elapsed().as_millis() as u64)
                        .unwrap();
                    last_publish = Instant::now();
                    // slow consumer every 100 messages
                    if seq % 100 == 0 && self.config.sleep_sub != 0 {
                        println!("sleeping {} seconds ...", self.config.sleep_sub);
                        time::sleep(Duration::from_secs(self.config.sleep_sub)).await;
                    }
                }
                Event::Incoming(Incoming::PingResp) | Event::Outgoing(_) => {}
                incoming => error!(
                    "Id = {}, Unexpected incoming packet = {:?}",
                    self.id, incoming
                ),
            }
        }

        let outgoing_throughput =
            (publish_count * 1000) as f32 / (last_publish - start).as_millis() as f32;

        // println!(
        //     "Id = {}
        //     Throughputs
        //     ----------------------------
        //     Incoming publishes : {:<7} Throughput = {} messages/s
        //     Outgoing pubacks   : Sent = {}
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
        //     publish_count,
        //     outgoing_throughput,
        //     puback_count,
        //     reconnects,
        //     histogram.len(),
        //     histogram.value_at_percentile(100.0),
        //     histogram.value_at_percentile(99.9999),
        //     histogram.value_at_percentile(99.999),
        //     histogram.value_at_percentile(90.0),
        //     histogram.value_at_percentile(50.0),
        // );

        SubStats {
            publish_count: publish_count as u64,
            puback_count,
            reconnects,
            throughput: outgoing_throughput,
        }
    }
}
