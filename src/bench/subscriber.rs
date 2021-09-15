use std::{sync::Arc, time::Instant};

use hdrhistogram::Histogram;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming};

use crate::{
    bench::{get_qos, options, ConnectionError},
    BenchConfig,
};

pub struct Subscriber {
    id: String,
    config: Arc<BenchConfig>,
    #[allow(dead_code)]
    client: AsyncClient,
    eventloop: EventLoop,
}

impl Subscriber {
    pub(crate) async fn new(id: String, config: Arc<BenchConfig>) -> Result<Subscriber, ConnectionError> {
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
            .subscribe("hello/+/world", get_qos(config.subscribe_qos))
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

    pub(crate) async fn start(&mut self) {
        let required_publish_count = self.config.count;
        // total number of publishes received
        let mut count = 0;
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
                    count += 1;
                    start = Instant::now();
                    last_publish = start;
                    break;
                }
                Event::Incoming(Incoming::PingResp) | Event::Outgoing(_) => {}
                incoming => {
                    error!("Id = {}, Unexpected incoming packet = {:?}", self.id, incoming);
                    continue;
                }
            }
        }

        // for remainging publishes
        while count < required_publish_count {
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

            debug!("Id = {}, {:?}, count = {}", self.id, event, count);

            match event {
                Event::Incoming(Incoming::Publish(_)) => {
                    count += 1;
                    histogram
                        .record(last_publish.elapsed().as_millis() as u64)
                        .unwrap();
                    last_publish = Instant::now();
                }
                Event::Incoming(Incoming::PingResp) | Event::Outgoing(_) => {}
                incoming => error!("Id = {}, Unexpected incoming packet = {:?}", self.id, incoming),
            }
        }

        let outgoing_throughput = (count * 1000) as f32 / (last_publish - start).as_millis() as f32;

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
            count,
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
