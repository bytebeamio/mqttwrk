use rumqttc::{MqttOptions, AsyncClient, Incoming, Event, EventLoop, Outgoing, QoS};
use tokio::task;
use colored::*;

pub async fn start() {
    idle_connection().await;
    publishes().await;
}

pub async fn idle_connection() {
    println!("\n{}\n", "Running ping test".yellow().bold().underline());
    let (_client, mut eventloop) = AsyncClient::new(options("test-client", 5, 10), 10);
    connect(&mut eventloop).await;

    for _i in 0..count {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Incoming::PingResp) => blue_ln!("Recv ping response"),
            Event::Outgoing(Outgoing::PingReq) => white_ln!("Sent ping request"),
            event => {
                red_ln!("Unexpected event: {:?}", event);
                break
            }
        }
    }
}

pub async fn publishes() {
    println!("\n{}\n", "Running publish test".yellow().bold().underline());
    let (client, mut eventloop) = AsyncClient::new(options("test-client", 5, 10), 10);
    connect(&mut eventloop).await;

    task::spawn(async move {
        for i in 0..10 {
            client.publish("hello/1/world", QoS::AtMostOnce, false, vec![i, 2, 3]).await.unwrap();
        }

        for i in 0..10 {
            client.publish("hello/1/world", QoS::AtLeastOnce, false, vec![i, 2, 3]).await.unwrap();
        }
    });

    // 10 qos 0 publishes, 10 qos 1 publishes, 10 pubacks, 2 pingreq, 2 ping resp
    for _i in 0..34 {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Incoming::PubAck(ack)) => println!("{} ({})", "Recv puback".green(), ack.pkid),
            Event::Outgoing(Outgoing::Publish(pkid)) => println!("{} ({})", "Sent publish".white(), pkid),
            Event::Incoming(Incoming::PingResp) => println!("{}", "Recv ping response".green()),
            Event::Outgoing(Outgoing::PingReq) => println!("{}", "Sent ping request".white()),
            event => {
                red_ln!("Unexpected event: {:?}", event);
                break
            }
        }
    }
}

async fn connect(eventloop: &mut EventLoop) {
    let event = eventloop.poll().await.unwrap();
    if let Event::Incoming(v) = event {
        match v {
            Incoming::ConnAck(_) => return,
            incoming => unreachable!("Expecting connack packet. Received = {:?}", incoming)
        }
    }
}

fn options(id: &str, keep_alive: u16, max_inflight: u16) -> MqttOptions {
    let mut options = MqttOptions::new(id, "localhost", 1883);
    options.set_keep_alive(keep_alive);
    options.set_inflight(max_inflight);
    options.set_connection_timeout(10);
    options
}