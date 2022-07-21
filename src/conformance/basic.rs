#![allow(unused_imports)]

use crate::conformance::common::{self, WrappedEvent, WrappedEventLoop};
use colored::Colorize;
use rumqttc::{
    AsyncClient, ConnAck, ConnectReturnCode, Event, Incoming, MqttOptions, Outgoing, Packet,
    PubAck, Publish, QoS, SubAck, Subscribe, SubscribeReasonCode,
};
use std::thread;
use std::time::Duration;

// TODO?: Connecting to same socket twice should fail
pub async fn test_basic() {
    println!("{}", "Basic test".yellow());
    let config = common::mqtt_config();

    let (client, connection) = AsyncClient::new(config.clone(), 10);
    drop(client);
    drop(connection);

    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);

    client.subscribe("topic/a", QoS::ExactlyOnce).await.unwrap();
    let notification1 = eventloop.poll().await.unwrap(); // connack
    let notification2 = eventloop.poll().await.unwrap(); // suback

    assert_eq!(
        notification1,
        Event::Incoming(Packet::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success
        }))
    );
    assert_eq!(
        notification2,
        Event::Incoming(Incoming::SubAck(SubAck {
            pkid: 1,
            return_codes: [SubscribeReasonCode::Success(QoS::ExactlyOnce)].to_vec(),
        }))
    );

    client
        .publish("topic/a", QoS::AtMostOnce, false, "QoS::AtMostOnce")
        .await
        .unwrap();

    let notification1 = eventloop.poll().await.unwrap(); // incoming:publish

    // Because we have subscribed to this topic
    assert!(WrappedEvent::new(notification1).is_publish().evaluate());

    client
        .publish("topic/a", QoS::AtLeastOnce, false, "QoS::AtLeastOnce")
        .await
        .unwrap();

    let notification1 = eventloop.poll().await.unwrap();
    let notification2 = eventloop.poll().await.unwrap();
    assert_eq!(
        notification1,
        Event::Incoming(Incoming::PubAck(PubAck { pkid: 2 }))
    );
    assert!(WrappedEvent::new(notification2).is_publish().evaluate());

    println!("{}", "Basic test succedeed".green())
}

pub async fn test_retained_messages() {
    let config = common::mqtt_config();
    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);

    println!("{}", "Retained message test".green());
    let qos0topic = "fromb/qos 0";
    let qos1topic = "fromb/qos 1";
    let qos2topic = "fromb/qos2";
    let wildcardtopic = "fromb/+";

    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Event::Incoming(Packet::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success
        }))
    );

    client
        .publish(qos0topic, QoS::AtMostOnce, true, "QoS::AtMostOnce")
        .await
        .unwrap();

    client
        .publish(qos1topic, QoS::AtLeastOnce, true, "QoS::AtLeastOnce")
        .await
        .unwrap();

    client
        .publish(qos2topic, QoS::ExactlyOnce, false, "QoS::ExactlyOnce")
        .await
        .unwrap();

    client
        .subscribe(wildcardtopic, QoS::ExactlyOnce)
        .await
        .unwrap();

    let notif1 = eventloop.poll().await.unwrap();
    let notif2 = eventloop.poll().await.unwrap();
    let notif3 = eventloop.poll().await.unwrap();
    match (notif1, notif2, notif3) {
        (
            Event::Incoming(Incoming::Publish(_)),
            Event::Incoming(Incoming::Publish(_)),
            Event::Incoming(Incoming::Publish(_)),
        ) => {}
        _ => {
            panic!("Expected 3 Publish messages");
        }
    }
    drop(client);
    drop(eventloop);

    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);

    client
        .publish(qos0topic, QoS::AtMostOnce, true, "")
        .await
        .unwrap();

    client
        .publish(qos1topic, QoS::AtLeastOnce, true, "")
        .await
        .unwrap();

    let notif1 = eventloop.poll().await.unwrap(); // connack
    let notif2 = eventloop.poll().await.unwrap();

    assert_eq!(
        notif1,
        Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        }))
    );
    assert_eq!(notif2, Event::Incoming(Incoming::PingResp))
}
