use colored::*;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, Outgoing, QoS};
use std::time::Duration;
use tokio::{task, time};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub async fn start() {
    // idle_connection().await;
    // publishes().await;
    subscribe().await;
}

#[allow(dead_code)]
pub async fn idle_connection() {
    println!("\n{}\n", "Running ping test".yellow().bold().underline());
    let (_client, mut eventloop) = AsyncClient::new(options("test-client", 5, 10), 10);
    connect(&mut eventloop).await;

    for _i in 0..6 {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Incoming::PingResp) => blue_ln!("Recv ping response"),
            Event::Outgoing(Outgoing::PingReq) => white_ln!("Sent ping request"),
            event => {
                red_ln!("Unexpected event: {:?}", event);
                break;
            }
        }
    }
}

#[allow(dead_code)]
pub async fn publishes() {
    println!("\n{}\n", "Running publish test".yellow().bold().underline());
    let (client, mut eventloop) = AsyncClient::new(options("test-client", 5, 10), 10);
    connect(&mut eventloop).await;

    task::spawn(async move {
        for i in 0..10 {
            client
                .publish("hello/1/world", QoS::AtMostOnce, false, vec![i, 2, 3])
                .await
                .unwrap();
        }

        for i in 0..10 {
            client
                .publish("hello/1/world", QoS::AtLeastOnce, false, vec![i, 2, 3])
                .await
                .unwrap();
        }
    });

    // 10 qos 0 publishes, 10 qos 1 publishes, 10 pubacks, 2 pingreq, 2 ping resp
    for _i in 0..34usize {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Incoming::PubAck(ack)) => {
                println!("{} ({})", "Recv puback".green(), ack.pkid)
            }
            Event::Outgoing(Outgoing::Publish(pkid)) => {
                println!("{} ({})", "Sent publish".white(), pkid)
            }
            Event::Incoming(Incoming::PingResp) => println!("{}", "Recv ping response".green()),
            Event::Outgoing(Outgoing::PingReq) => println!("{}", "Sent ping request".white()),
            event => {
                red_ln!("Unexpected event: {:?}", event);
                break;
            }
        }
    }
}

pub async fn subscribe() {
    println!(
        "\n{}\n",
        "Running subscribe test".yellow().bold().underline()
    );
    let (client1, mut eventloop1) = AsyncClient::new(options("test-client-1", 5, 10), 10);
    connect(&mut eventloop1).await;

    // Client 1 publish task
    task::spawn(async move {
        for i in 0..10 {
            time::sleep(Duration::from_secs(1)).await;
            client1
                .publish("hello/1/world", QoS::AtLeastOnce, false, vec![i, 2, 3])
                .await
                .unwrap();
        }
    });

    // Client 1 incoming data
    task::spawn(async move {
        // 10 qos 1 publishes, 10 pubacks, 2 pingreq, 2 ping resp
        for _i in 0..24usize {
            match eventloop1.poll().await.unwrap() {
                Event::Incoming(i) => match i {
                    Incoming::PubAck(ack) => {
                        println!("{} ({})", "1. Recv puback".green(), ack.pkid)
                    }
                    Incoming::PingResp => println!("{}", "1. Recv ping response".green()),
                    event => {
                        red_ln!("Unexpected incoming event: {:?}", event);
                        break;
                    }
                },
                Event::Outgoing(o) => match o {
                    Outgoing::Publish(pkid) => println!("{} ({})", "1. Sent publish".white(), pkid),
                    Outgoing::PingReq => println!("{}", "1. Sent ping request".white()),
                    event => {
                        red_ln!("Unexpected outgoing event: {:?}", event);
                        break;
                    }
                },
            }
        }
    });

    let (client2, mut eventloop2) = AsyncClient::new(options("test-client-2", 5, 10), 10);
    connect(&mut eventloop2).await;
    client2
        .subscribe("hello/+/world", QoS::AtLeastOnce)
        .await
        .unwrap();

    // 10 qos 1 publishes, 10 pubacks, 2 pingreq, 2 ping resp
    for _i in 0..24usize {
        // 1 outgoing subscribe
        // 1 incoming suback
        // 10 incoming qos 1 publishes
        // 10 outgoing pubacks,
        // 2 outgoing pingreq,
        // 2 incoming pingresp
        match eventloop2.poll().await.unwrap() {
            Event::Incoming(i) => match i {
                Incoming::SubAck(ack) => println!("{} ({})", "2. Recv suback".green(), ack.pkid),
                Incoming::Publish(p) => println!("{} ({})", "2. Recv publish".green(), p.pkid),
                Incoming::PingResp => println!("{}", "2. Recv ping response".green()),
                event => {
                    red_ln!("Unexpected incoming event: {:?}", event);
                    break;
                }
            },
            Event::Outgoing(o) => match o {
                Outgoing::Subscribe(pkid) => println!("{} ({})", "2. Sent subscribe".white(), pkid),
                Outgoing::PubAck(pkid) => println!("{} ({})", "2. Sent puback".white(), pkid),
                Outgoing::PingReq => println!("{}", "2.. Sent ping request".white()),
                event => {
                    red_ln!("Unexpected outgoing event: {:?}", event);
                    break;
                }
            },
        }
    }
}

async fn connect(eventloop: &mut EventLoop) {
    let event = eventloop.poll().await.unwrap();
    if let Event::Incoming(v) = event {
        match v {
            Incoming::ConnAck(_) => (),
            incoming => unreachable!("Expecting connack packet. Received = {:?}", incoming),
        }
    }
}

fn options(id: &str, keep_alive: u64, max_inflight: u16) -> MqttOptions {
    let mut options = MqttOptions::new(id, "localhost", 1883);
    options.set_keep_alive(Duration::from_secs(keep_alive));
    options.set_inflight(max_inflight);
    options.set_connection_timeout(10);
    options.set_clean_session(false);
    options
}
