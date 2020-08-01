use rumqttc::{MqttOptions, EventLoop, Request, QoS, Incoming};
use std::time::{Duration, Instant};
use std::collections::HashSet;
use std::io;
use std::io::prelude::*;
use std::fs::File;

use rand::Rng;
use tokio::task;
use tokio::time;
use async_channel::Sender;

use crate::Metrics;

fn set_tls(mqttoptions: &mut MqttOptions, ca_file: Option<String>, client_file: Option<String>, client_key: Option<String>, use_ssl: i16) {
    let mut ca_data: Vec<u8> = Vec::new();
    let mut cert_data = Vec::new();
    let mut key_data = Vec::new();

    println!("Inside set_tls");

    // Unwrap or handle?
    match ca_file {
        Some(file_path) => {
            let mut ca_file = File::open(file_path).unwrap();
            ca_file.read_to_end(&mut ca_data).unwrap();
            
        },
        None => println!("No ca file provided"),
    }
    mqttoptions.set_ca(ca_data);

    match client_file {
        Some(file_path) => {
            let mut cert_file = File::open(file_path).unwrap();
            cert_file.read_to_end(&mut cert_data).unwrap();
        },
        None => println!("No client_cert privided"),
    };
    match client_key {
        Some(file_path) => {
           let mut key_file = File::open(file_path).unwrap();
           key_file.read_to_end(&mut key_data).unwrap();
        },
        None => println!("No client key provided"),
    };
    if use_ssl == 2 {
        mqttoptions.set_client_auth(cert_data, key_data);
    }
}

pub async fn start(id: &str, payload_size: usize, count: u16, server: String, port: u16,
        keep_alive: u16, inflight: u16, use_ssl: i16, ca_file: Option<String>,
        client_cert: Option<String>, client_key: Option<String>) {
    let mut mqttoptions = MqttOptions::new(id, server, port);
    mqttoptions.set_keep_alive(keep_alive);
    mqttoptions.set_inflight(inflight);
    
    match use_ssl {
        1 => set_tls(&mut mqttoptions, ca_file, client_cert, client_key, 1),  
        2 => set_tls(&mut mqttoptions, ca_file, client_cert, client_key, use_ssl),    // set ca as well as client cert and key
        _ => {},
    };

    let mut eventloop = EventLoop::new(mqttoptions, 10).await;
    let requests_tx = eventloop.handle();

    let client_id = id.to_owned();
    task::spawn(async move {
        requests(&client_id, payload_size, count, requests_tx).await;
    });

    let mut acks = acklist(count);
    let mut incoming = acklist(count);
    let mut data = Metrics {
        progress: 0,
    };

    let start = Instant::now();
    let mut acks_elapsed_ms = 0;
    let mut reconnects:i32 = 0;
    let mut reconnect_threshold:i32 = 0;

    loop {
        let res =  match eventloop.poll().await{
            Ok(v) => v,
            Err(_e) => {
                reconnects = reconnects + 1;
                reconnect_threshold += 1;
                // 1000 continous reconnect, break
                if reconnect_threshold == 1000 {
                    break;
                }
                continue;
            },
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
                    },
                    Incoming::SubAck(suback)=> {
                        acks.remove(&suback.pkid);
                    },
                    Incoming::Publish(publish) => {
                        data.progress = publish.pkid;
                        incoming.remove(&publish.pkid);
                    },
                    v => {
                        println!("Incoming={:?}", v);
                        continue;
                    },
                }
            },
            None => {},
        }

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
        Incoming: Missed = {:<5}, Received size = {}, Incoming Throughput = {} KB/s
        Reconnects: {}",
        id,
        acks.len(),
        total_outgoing_size,
        acks_throughput_mbps,
        incoming.len(),
        total_incoming_size,
        incoming_throughput_mbps,
        reconnects,
    );
}

async fn requests(id: &str, payload_size: usize, count: u16, requests_tx: Sender<Request>) {
    let topic = format!("hello/{}/world", id);
    let subscription = rumqttc::Subscribe::new(&topic, QoS::AtLeastOnce);
    let _ = requests_tx.send(Request::Subscribe(subscription)).await;

    for i in 0..count {
        let mut payload = generate_payload(payload_size);
        payload[0] = (i % 255) as u8;
        let publish = rumqttc::Publish::new(&topic, QoS::AtLeastOnce, payload);
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