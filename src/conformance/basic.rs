#![allow(unused_imports)]

use crate::conformance::common::{self, WrappedEvent, WrappedEventLoop};
use colored::Colorize;
use rumqttc::{
    AsyncClient, ConnAck, ConnectReturnCode, Event, Incoming, LastWill, MqttOptions, Outgoing,
    Packet, PubAck, Publish, QoS, SubAck, Subscribe, SubscribeFilter, SubscribeReasonCode,
};
use std::thread;
use std::time::Duration;

pub async fn session_test() {
    println!("{}", "Session test".yellow());
    let mut config = common::mqtt_config(1);

    config.set_clean_session(false);

    let (client, mut eventloop) = AsyncClient::new(config.clone(), 10);
    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Event::Incoming(Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        }))
    );

    client.subscribe("topic/a", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // suback
    drop(client);
    drop(eventloop);

    let (_client, mut eventloop) = AsyncClient::new(config.clone(), 10);
    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Event::Incoming(Packet::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success
        }))
    );
    println!("{}", "Session test Successful".yellow());
}

// TODO?: Connecting to same socket twice should fail
pub async fn test_basic() {
    println!("{}", "Basic test".yellow());
    let config = common::mqtt_config(1);

    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    drop(client);
    drop(eventloop);

    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);

    #[cfg(feature = "qos2")]
    {
        client.subscribe("topic/a", QoS::ExactlyOnce).await.unwrap();
    }
    #[cfg(not(feature = "qos2"))]
    {
        client.subscribe("topic/a", QoS::AtMostOnce).await.unwrap();
    }
    let notification1 = eventloop.poll().await.unwrap(); // connack
    let notification2 = eventloop.poll().await.unwrap(); // suback

    assert_eq!(
        notification1,
        Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        }))
    );
    #[cfg(feature = "qos2")]
    assert_eq!(
        notification2,
        Event::Incoming(Incoming::SubAck(SubAck {
            pkid: 1,
            return_codes: [SubscribeReasonCode::Success(QoS::ExactlyOnce)].to_vec(),
        }))
    );
    #[cfg(not(feature = "qos2"))]
    assert_eq!(
        notification2,
        Event::Incoming(Incoming::SubAck(SubAck {
            pkid: 1,
            return_codes: [SubscribeReasonCode::Success(QoS::AtMostOnce)].to_vec(),
        }))
    );

    client
        .publish("topic/a", QoS::AtMostOnce, false, "QoS::AtMostOnce")
        .await
        .unwrap();

    let notification1 = eventloop.poll().await.unwrap(); // incoming:publish

    // Because we have subscribed to this topic
    assert!(WrappedEvent::new(notification1).is_publish().evaluate());

    #[cfg(feature = "qos2")]
    {
        let _ = eventloop.poll().await.unwrap(); // incoming:pubrec
        let _ = eventloop.poll().await.unwrap(); // incoming:pubcomp
    }

    client
        .publish("topic/a", QoS::AtLeastOnce, false, "QoS::AtLeastOnce")
        .await
        .unwrap();

    let notification1 = eventloop.poll().await.unwrap();
    assert_eq!(
        notification1,
        Event::Incoming(Incoming::PubAck(PubAck { pkid: 2 }))
    );
    let notification2 = eventloop.poll().await.unwrap();
    assert!(WrappedEvent::new(notification2).is_publish().evaluate());

    #[cfg(feature = "qos2")]
    {
        client
            .publish("topic/a", QoS::ExactlyOnce, false, "QoS::ExactlyOnce")
            .await
            .unwrap();
    }

    println!("{}", "Basic test succedeed".green())
}

pub async fn test_keepalive() {
    let mut config = common::mqtt_config(1);
    config
        .set_last_will(LastWill::new(
            "topic/will",
            "client disconnected",
            QoS::AtMostOnce,
            false,
        ))
        .set_keep_alive(Duration::from_secs(5));
    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);
    let _ = eventloop.poll().await.unwrap(); // connack
}

// messages not being retained
pub async fn test_retained_messages() {
    let config = common::mqtt_config(1);
    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);

    println!("{}", "Retained message test".yellow());
    let qos0topic = "fromb/qos 0";
    let qos1topic = "fromb/qos 1";
    let qos2topic = "fromb/qos2";
    let wildcardtopic = "fromb/+";

    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Event::Incoming(Packet::ConnAck(ConnAck {
            session_present: false,
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

    let _ = eventloop.poll().await.unwrap(); // incoming: puback

    #[cfg(feature = "qos2")]
    {
        client
            .publish(qos2topic, QoS::ExactlyOnce, true, "QoS::ExactlyOnce")
            .await
            .unwrap();

        let _ = eventloop.poll().await.unwrap(); // incoming: pubrec
        let _ = eventloop.poll().await.unwrap(); // incoming: pubcomp
    }

    client
        .subscribe(wildcardtopic, QoS::AtMostOnce)
        .await
        .unwrap();
    let _ = eventloop.poll().await.unwrap(); //suback

    let notif1 = eventloop.poll().await.unwrap();
    dbg!(notif1.clone());
    assert!(WrappedEvent::new(notif1).is_publish().evaluate());

    let notif2 = eventloop.poll().await.unwrap();
    assert!(WrappedEvent::new(notif2).is_publish().evaluate());

    #[cfg(feature = "qos2")]
    {
        let notif3 = eventloop.poll().await.unwrap();
        assert!(WrappedEvent::new(notif3).is_publish().evaluate());
    }

    drop(client);
    drop(eventloop);

    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);

    let notif1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notif1,
        Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        }))
    );

    client
        .publish(qos0topic, QoS::AtMostOnce, true, "")
        .await
        .unwrap();

    client
        .publish(qos1topic, QoS::AtLeastOnce, true, "")
        .await
        .unwrap();

    let _ = eventloop.poll().await.unwrap(); // incoming: puback

    #[cfg(feature = "qos2")]
    {
        client
            .publish(qos1topic, QoS::ExactlyOnce, true, "")
            .await
            .unwrap();

        let _ = eventloop.poll().await.unwrap(); // incoming: pubrec
        let _ = eventloop.poll().await.unwrap(); // incoming: pubcomp
    }

    let notif2 = eventloop.poll().await.unwrap();

    // We cleared all the retained messages so should only receive pings
    assert_eq!(notif2, Event::Incoming(Incoming::PingResp));
    println!("{}", "Retained message test Successfull".green());
}

pub async fn test_zero_length_clientid() {
    let mut config = MqttOptions::new("", "localhost", 1883);
    config.set_clean_session(true);

    let (_client, mut eventloop) = common::get_client(config);

    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Event::Incoming(Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        }))
    );

    let mut config2 = MqttOptions::new("", "localhost", 1883);
    config2.set_clean_session(false);

    let (_client, mut eventloop) = common::get_client(config2);

    let notification1 = eventloop.poll().await.unwrap(); // connack
    assert_eq!(
        notification1,
        Event::Incoming(Packet::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::BadClientId
        }))
    );
}

pub async fn test_offline_message_queueing() {
    println!("{}", "Offline message Queue test".yellow());
    let mut config = common::mqtt_config(1);
    config.set_clean_session(false);

    let (client1, mut eventloop1) = common::get_client(config.clone());
    let _ = eventloop1.poll().await.unwrap(); // connack

    client1.subscribe("+/+", QoS::AtLeastOnce).await.unwrap();
    let _ = eventloop1.poll().await.unwrap(); // suback
    drop(eventloop1);

    let mut config2 = common::mqtt_config(2);
    config2.set_clean_session(false);

    let (client2, mut eventloop2) = common::get_client(config2);
    let _ = eventloop2.poll().await.unwrap(); // connack

    client2
        .publish("topic/a", QoS::AtMostOnce, true, "QoS::AtMostOnce")
        .await
        .unwrap();

    client2
        .publish("topic/a", QoS::AtLeastOnce, true, "QoS::AtLeastOnce")
        .await
        .unwrap();

    let _ = eventloop2.poll().await.unwrap(); // incoming: puback

    #[cfg(feature = "qos2")]
    {
        client2
            .publish("topic/a", QoS::ExactlyOnce, true, "QoS::ExactlyOnce")
            .await
            .unwrap();

        let _ = eventloop2.poll().await.unwrap(); // incoming: pubrec
        let _ = eventloop2.poll().await.unwrap(); // incoming: pubcomp
    }

    client2.disconnect().await.unwrap();

    let (_client1, mut eventloop1) = common::get_client(config.clone());
    let notif1 = eventloop1.poll().await.unwrap(); // connack

    assert_eq!(
        notif1,
        Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success
        }))
    );

    // TODO: We don't store QoS0 publish's when client is not connected. Some clients might expect
    // broker to store it.
    let notif1 = eventloop1.poll().await.unwrap(); // QoS1 publish
    let notif2 = eventloop1.poll().await.unwrap(); // PingResp
    dbg!(notif1.clone(), notif2.clone());
    assert!(WrappedEvent::new(notif1).is_publish().evaluate());
    assert!(WrappedEvent::new(notif2).is_pingresp().evaluate());
    println!("{}", "Offline message Queue test successful".green());
    #[cfg(feature = "qos2")]
    {
        let notif3 = eventloop1.poll().await.unwrap(); // QoS2 publish
    }
}

// This test failing when ran on broker with no old session
pub async fn test_overlapping_subscriptions() {
    println!("{}", "Overlapping subscriptions test".yellow());
    let mut config = common::mqtt_config(1);
    config.set_clean_session(false);

    let (client, mut eventloop) = common::get_client(config.clone());
    let _ = eventloop.poll().await.unwrap(); // connack

    client
        .subscribe_many(vec![
            SubscribeFilter::new("topic/+".to_string(), QoS::AtMostOnce),
            SubscribeFilter::new("topic/#".to_string(), QoS::AtLeastOnce),
        ])
        .await
        .unwrap();

    let _ = eventloop.poll().await.unwrap(); // suback
    client
        .publish(
            "topic/a",
            QoS::AtMostOnce,
            false,
            "overlapping topic filter",
        )
        .await
        .unwrap();

    loop {
        let notif1 = eventloop.poll().await.unwrap(); // publish from topic/+
        dbg!(notif1);
    }
    // let notif2 = eventloop.poll().await.unwrap(); // publish from topic/#
    // assert!(WrappedEvent::new(notif1).is_publish().evaluate());
    // assert!(WrappedEvent::new(notif2).is_publish().evaluate());
    println!("{}", "Overlapping subscriptions test Successful".green());
}

pub async fn test_will_message() {
    println!("{}", "Will message test".yellow());
    let mut config: MqttOptions = common::mqtt_config(1);
    config
        .set_last_will(LastWill::new(
            "topic/will",
            "client disconnected",
            QoS::AtLeastOnce,
            false,
        ))
        .set_keep_alive(Duration::from_secs(5));
    let (_client, mut eventloop) = common::get_client(config);

    let notif1 = eventloop.poll().await.unwrap(); // connack
    assert_eq!(
        notif1,
        Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        }))
    );

    let config2: MqttOptions = common::mqtt_config(2);
    let (client2, mut eventloop2) = common::get_client(config2);

    let _ = eventloop2.poll().await.unwrap(); // connack

    client2
        .subscribe("topic/will", QoS::AtMostOnce)
        .await
        .unwrap();

    let _ = eventloop2.poll().await.unwrap(); // suback

    drop(eventloop);

    thread::sleep(Duration::from_secs(10));

    let notif3 = eventloop2.poll().await.unwrap(); // publish

    assert!(WrappedEvent::new(notif3).is_publish().evaluate());
    println!("{}", "Will message test Successful".green());
}

pub async fn test_dollar_topic_filter() {
    println!("{}", "Subscribe failure test".yellow());
    let config: MqttOptions = common::mqtt_config(1);

    let (client1, mut eventloop1) = common::get_client(config.clone());
    let _ = eventloop1.poll().await.unwrap(); // connack

    client1.subscribe("+/+", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop1.poll().await.unwrap(); // suback

    client1
        .publish("$dollar_test", QoS::AtMostOnce, false, "")
        .await
        .unwrap();
    let _ = eventloop1.poll().await.unwrap(); // puback

    let notif1 = eventloop1.poll().await.unwrap();
    assert!(WrappedEvent::new(notif1).is_pingresp().evaluate());
    println!("{}", "Subscribe failure test Successful".green());
}

pub async fn test_unsubscribe() {
    println!("{}", "Subscribe failure test".yellow());
    let config: MqttOptions = common::mqtt_config(1);

    let (client1, mut eventloop1) = common::get_client(config.clone());
    let _ = eventloop1.poll().await.unwrap(); // connack

    client1.subscribe("topicA", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop1.poll().await.unwrap(); // suback

    client1
        .subscribe("topicA/B", QoS::AtMostOnce)
        .await
        .unwrap();
    let _ = eventloop1.poll().await.unwrap(); // suback

    client1.subscribe("topicC", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop1.poll().await.unwrap(); // suback

    client1.unsubscribe("topicA").await.unwrap();
    let notif1 = eventloop1.poll().await.unwrap(); // suback
    dbg!(notif1);

    let config: MqttOptions = common::mqtt_config(2);

    let (client2, mut eventloop2) = common::get_client(config.clone());
    let _ = eventloop2.poll().await.unwrap(); // connack

    client2
        .publish("topicA", QoS::AtMostOnce, false, "")
        .await
        .unwrap();
    let _ = eventloop2.poll().await.unwrap(); // puback
    client2
        .publish("topicA/B", QoS::AtMostOnce, false, "")
        .await
        .unwrap();
    let _ = eventloop2.poll().await.unwrap(); // puback
    client2
        .publish("topicC", QoS::AtMostOnce, false, "")
        .await
        .unwrap();
    let _ = eventloop2.poll().await.unwrap(); // puback

    let notif1 = eventloop1.poll().await.unwrap();
    assert!(WrappedEvent::new(notif1).is_publish().evaluate());
    let notif2 = eventloop1.poll().await.unwrap();
    assert!(WrappedEvent::new(notif2).is_publish().evaluate());
    let notif3 = eventloop1.poll().await.unwrap();
    assert!(WrappedEvent::new(notif3).is_pingresp().evaluate());
    println!("{}", "Subscribe failure test Successful".green());
}

pub async fn test_subscribe_failure() {
    println!("{}", "Subscribe failure test".yellow());
    let config: MqttOptions = common::mqtt_config(1);

    let (client, mut eventloop) = common::get_client(config.clone());
    let _ = eventloop.poll().await.unwrap(); // connack

    client
        .subscribe("test/shouldfail", QoS::AtMostOnce)
        .await
        .unwrap();

    // TODO: Err should contain more descriptive message
    assert!(eventloop.poll().await.is_err());
    println!("{}", "Subscribe failure test Successful".green());
}

pub async fn test_redelivery_on_reconnect() {
    // Need ability to not respond to publish messages but process outgoing requests
    todo!();
}

pub async fn test_connack_with_clean_session() {
    // To make sure any of the previous tests doesn't affect this create a connection and drop it
    // immediately to clean any previous state
    let config = common::mqtt_config(1);
    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);
    client.subscribe("topic/a", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // connack
    drop(client);
    drop(eventloop);

    // Make a connection with clean_session false to create any random state
    let mut config = common::mqtt_config(1);
    config.set_clean_session(false);
    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);
    let _ = eventloop.poll().await.unwrap(); // connack
    client.subscribe("topic/a", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // suback
    drop(client);
    drop(eventloop);

    // Should have a session present
    let mut config = common::mqtt_config(1);
    config.set_clean_session(false);
    let (client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);

    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Event::Incoming(Packet::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success
        }))
    );
    drop(client);
    drop(eventloop);

    // Should drop previous_state if any and reply with session_present false
    let config = common::mqtt_config(1);
    let (_client, eventloop) = AsyncClient::new(config.clone(), 10);
    let mut eventloop = WrappedEventLoop::new(eventloop);

    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Event::Incoming(Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        }))
    );
}
