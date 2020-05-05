use rumq_client::{self, eventloop, MqttOptions, Notification, PacketIdentifier, QoS, Request};
use std::collections::HashSet;
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::select;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task;
use tokio::time;

use crate::Metrics;

pub async fn start(id: &str, payload_size: usize, count: u16) {
    let (requests_tx, requests_rx) = channel(10);
    let mut mqttoptions = MqttOptions::new(id, "localhost", 1883);
    mqttoptions.set_keep_alive(5);

    // NOTE More the inflight size, better the perf
    mqttoptions.set_inflight(200);

    let mut eventloop = eventloop(mqttoptions, requests_rx);
    let client_id = id.to_owned();
    task::spawn(async move {
        requests(&client_id, payload_size, count, requests_tx).await;
    });

    let mut stream = eventloop.connect().await.unwrap();
    let mut acks = acklist(count);
    let mut incoming = acklist(count);
    let mut interval = time::interval(Duration::from_secs(1));
    let mut data = Metrics {
        progress: 0,
    };

    let start = Instant::now();
    let mut acks_elapsed_ms = 0;

    loop {
        let notification = select! {
            notification = stream.next() => match notification {
                Some(notification) => notification,
                None => break
            },
            _ = interval.tick() => {
                // println!("Id = {},  Progress = {:?}", id, data.progress);
                continue;
            }
        };

        match notification {
            Notification::Puback(pkid) => {
                let PacketIdentifier(pkid) = pkid;
                acks.remove(&pkid);
                acks_elapsed_ms = start.elapsed().as_millis();
                // println!("Id = {}, Elapsed = {:?}, Pkid = {:?}", id, start.elapsed().as_millis(), pkid);
                // start = Instant::now();
                continue;
            }
            Notification::Suback(suback) => {
                let PacketIdentifier(pkid) = suback.pkid;
                acks.remove(&pkid);
                suback.pkid;
            }
            Notification::Publish(publish) => {
                let PacketIdentifier(pkid) = publish.pkid.unwrap();
                data.progress = pkid;
                incoming.remove(&pkid);
            }
            notification => {
                println!("Id = {}, Notification = {:?}", id, notification);
                continue;
            }
        };

        if incoming.len() == 0 {
            break;
        }
    }

    let incoming_elapsed_ms = start.elapsed().as_millis();
    let incoming_count = count - incoming.len() as u16;
    let total_incoming_size = payload_size * incoming_count as usize;
    let incoming_throughput = total_incoming_size / incoming_elapsed_ms as usize;
    let incoming_throughput_mbps = incoming_throughput * 1000 / 1024;

    let acks_count = count - acks.len() as u16;
    let total_outgoing_size = payload_size * acks_count as usize;
    let acks_throughput = total_outgoing_size / acks_elapsed_ms as usize;
    let acks_throughput_mbps = acks_throughput * 1000 / 1024;

    println!(
        "Id = {},
        Acks:     Missed = {:<5}, Received size = {}, Incoming Throughput = {} KB/s,
        Incoming: Missed = {:<5}, Received size = {}, Incoming Throughput = {} KB/s",
        id,
        acks.len(),
        total_outgoing_size,
        acks_throughput_mbps,
        incoming.len(),
        total_incoming_size,
        incoming_throughput_mbps
    );
}

async fn requests(id: &str, payload_size: usize, count: u16, mut requests_tx: Sender<Request>) {
    let topic = format!("hello/{}/world", id);
    let subscription = rumq_client::Subscribe::new(&topic, QoS::AtLeastOnce);
    let _ = requests_tx.send(Request::Subscribe(subscription)).await;

    for i in 0..count {
        let mut payload = generate_payload(payload_size);
        payload[0] = (i % 255) as u8;
        let publish = rumq_client::Publish::new(&topic, QoS::AtLeastOnce, payload);
        let publish = Request::Publish(publish);
        if let Err(_) = requests_tx.send(publish).await {
            break;
        }
    }

    time::delay_for(Duration::from_secs(5)).await;
}

fn acklist(count: u16) -> HashSet<u16> {
    let mut acks = HashSet::new();
    for i in 1..=count {
        acks.insert(i);
    }

    acks
}

fn generate_payload(payload_size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let payload: Vec<u8> = (0..payload_size).map(|_| rng.gen_range(0, 255)).collect();
    payload
}
