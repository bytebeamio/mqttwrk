#![allow(unused_imports)]

use crate::common::{self, WrappedEventLoop};
use crate::conformance::PROGRESS_BAR;
use crate::ConformanceConfig;
use colored::Colorize;
use indicatif::ProgressBar;
use rumqttc::{
    matches, AsyncClient, ConnAck, ConnectReturnCode, Event, Incoming, LastWill, MqttOptions,
    Outgoing, Packet, PubAck, PubComp, PubRec, Publish, QoS, SubAck, Subscribe, SubscribeFilter,
    SubscribeReasonCode, UnsubAck,
};
use std::thread;
use std::time::Duration;

// TODO?: Connecting to same socket twice should fail
pub async fn test_basic(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Basic test".yellow().to_string());
    let mut config = MqttOptions::new(
        "conformance-basic",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));

    let (client, eventloop) = common::get_client(config.clone());
    drop(client);
    drop(eventloop);

    let (client, mut eventloop) = common::get_client(config.clone());

    let incoming = eventloop.poll().await.unwrap(); // connack
    assert_eq!(
        incoming,
        Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        })
    );

    client.subscribe("topic/q0", QoS::AtMostOnce).await.unwrap();
    let incoming = eventloop.poll().await.unwrap(); // suback
    assert_eq!(
        incoming,
        Incoming::SubAck(SubAck {
            pkid: 1,
            return_codes: [SubscribeReasonCode::Success(QoS::AtMostOnce)].to_vec(),
        })
    );

    client
        .subscribe("topic/q1", QoS::AtLeastOnce)
        .await
        .unwrap();
    let incoming = eventloop.poll().await.unwrap(); // suback
    assert_eq!(
        incoming,
        Incoming::SubAck(SubAck {
            pkid: 2,
            return_codes: [SubscribeReasonCode::Success(QoS::AtLeastOnce)].to_vec(),
        })
    );

    client
        .subscribe("topic/q2", QoS::ExactlyOnce)
        .await
        .unwrap();
    let incoming = eventloop.poll().await.unwrap(); // suback
    assert_eq!(
        incoming,
        Incoming::SubAck(SubAck {
            pkid: 3,
            return_codes: [SubscribeReasonCode::Success(QoS::ExactlyOnce)].to_vec(),
        })
    );

    // Qos 0 Publish
    client
        .publish("topic/q0", QoS::AtMostOnce, false, "QoS::AtMostOnce")
        .await
        .unwrap();

    let incoming = eventloop.poll().await.unwrap(); // incoming:publish
    assert!(matches!(incoming, Incoming::Publish(Publish { .. })));

    // Qos 1 Publish
    client
        .publish("topic/q1", QoS::AtLeastOnce, false, "QoS::AtLeastOnce")
        .await
        .unwrap();

    // Qos 2 Publish
    client
        .publish("topic/q2", QoS::ExactlyOnce, false, "QoS::ExactlyOnce")
        .await
        .unwrap();

    let incoming = eventloop.poll().await.unwrap(); // incoming:publish
    assert!(matches!(incoming, Incoming::PubAck(PubAck { .. })));

    let incoming = eventloop.poll().await.unwrap(); // incoming:publish
    assert!(matches!(incoming, Incoming::Publish(Publish { .. })));

    let incoming = eventloop.poll().await.unwrap(); // incoming:pubrec
    assert!(matches!(incoming, Incoming::PubRec(PubRec { .. })));

    let incoming = eventloop.poll().await.unwrap(); // incoming:pubcomp
    assert!(matches!(incoming, Incoming::PubComp(PubComp { .. })));

    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Basic test succedeed".green().to_string());
}

pub async fn session_test(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Session test".yellow().to_string());

    let mut config = MqttOptions::new(
        "conformance-session",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));
    config.set_clean_session(true);

    let (client, mut eventloop) = common::get_client(config.clone());
    let incoming = eventloop.poll().await.unwrap(); // connack
    assert!(matches!(incoming, Incoming::ConnAck(ConnAck { .. })));

    client.disconnect().await.unwrap();
    assert!(eventloop.poll().await.is_err());

    let mut config2 = MqttOptions::new(
        "conformance-session",
        &conformance_config.server,
        conformance_config.port,
    );
    config2.set_keep_alive(Duration::from_secs(5));
    config2.set_clean_session(false);

    let (client, mut eventloop) = common::get_client(config2.clone());
    let incoming = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        incoming,
        Packet::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success
        })
    );

    client.subscribe("topic/a", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // suback
    drop(client);
    drop(eventloop);

    let (_client, mut eventloop) = common::get_client(config.clone());
    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        })
    );
    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Session test successful".green().to_string());
}

pub async fn test_overlapping_subscriptions(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Overlapping subscriptions test".yellow().to_string());
    let mut config = MqttOptions::new(
        "conformance-overlapping-subscriptions",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));
    config.set_clean_session(true);

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
            QoS::AtLeastOnce,
            false,
            "overlapping topic filter",
        )
        .await
        .unwrap();
    let _ = eventloop.poll().await.unwrap(); // puback

    let notif1 = eventloop.poll().await.unwrap(); // publish from topic/+
    let notif2 = eventloop.poll().await.unwrap(); // publish from topic/#
                                                  // dbg!(&notif1, &notif2);

    let notif1_is_publish = matches!(notif1, Incoming::Publish(Publish { .. }));
    let notif2_is_publish = matches!(notif2, Incoming::Publish(Publish { .. }));

    match (notif1_is_publish, notif2_is_publish) {
        (true, false) | (false, true) => {
            PROGRESS_BAR.println(
                "Broker publishes 1 message per overlapping subscription"
                    .green()
                    .to_string(),
            );
        }
        (true, true) => PROGRESS_BAR.println(
            "Broker publishes 1 message for all matching subscription"
                .green()
                .to_string(),
        ),
        (false, false) => {
            panic!("Should receive atleast 1 publish message");
        }
    }
    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println(
        "Overlapping subscriptions test Successful"
            .green()
            .to_string(),
    );
}

// TODO: Not disconnecting the client if keep_alive time has passed with no messages from client
pub async fn test_keepalive(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Ping test".yellow().to_string());
    let mut config = MqttOptions::new(
        "conformance-overlapping-subscriptions",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));
    config.set_last_will(LastWill::new(
        "topic/will",
        "client disconnected",
        QoS::AtMostOnce,
        false,
    ));

    let (_client, mut eventloop) = common::get_client(config);
    let _ = eventloop.poll().await.unwrap(); // connack

    thread::sleep(Duration::from_secs(10));

    for i in 0..5 {
        let incoming = eventloop.poll().await.unwrap();
        assert!(matches!(incoming, Incoming::PingResp));
        PROGRESS_BAR.println(format!("Ping response {} received", i).green().to_string());
    }

    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Ping test successful".green().to_string());
}

pub async fn test_retain_on_different_connect(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message(
        "Retained message test on different connect"
            .yellow()
            .to_string(),
    );

    let mut config = MqttOptions::new(
        "conformance-retained-message",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = common::get_client(config.clone());

    let qos0topic = "fromb/qos 0";
    let qos1topic = "fromb/qos 1";
    let qos2topic = "fromb/qos 2";
    let wildcardtopic = "fromb/+";

    let notification1 = eventloop.poll().await.unwrap(); // connack
    assert_eq!(
        notification1,
        Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        })
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
        .publish(qos2topic, QoS::ExactlyOnce, true, "QoS::ExactlyOnce")
        .await
        .unwrap();

    let _ = eventloop.poll().await.unwrap(); // incoming: puback
    let _ = eventloop.poll().await.unwrap(); // incoming: pubrec
    let _ = eventloop.poll().await.unwrap(); // incoming: pubcomp

    client
        .subscribe(wildcardtopic, QoS::ExactlyOnce)
        .await
        .unwrap();

    let _ = eventloop.poll().await.unwrap(); //suback

    let notif1 = eventloop.poll().await.unwrap();
    // dbg!(&notif1);
    assert!(matches!(notif1, Incoming::Publish(Publish { .. })));

    let notif2 = eventloop.poll().await.unwrap();
    // dbg!(&notif2);
    assert!(matches!(notif2, Incoming::Publish(Publish { .. })));

    let notif3 = eventloop.poll().await.unwrap();
    // dbg!(&notif3);
    assert!(matches!(notif3, Incoming::Publish(Publish { .. })));

    drop(client);
    drop(eventloop);

    let mut config2 = MqttOptions::new(
        "conformance-retained-message2",
        &conformance_config.server,
        conformance_config.port,
    );
    config2.set_keep_alive(Duration::from_secs(5));

    let (client2, mut eventloop2) = common::get_client(config2.clone());

    let _ = eventloop2.poll().await.unwrap(); // connack

    client2
        .subscribe(wildcardtopic, QoS::AtMostOnce)
        .await
        .unwrap();

    let _ = eventloop2.poll().await.unwrap(); //suback

    let notif1 = eventloop2.poll().await.unwrap();
    assert!(matches!(notif1, Incoming::Publish(Publish { .. })));

    let notif2 = eventloop2.poll().await.unwrap();
    assert!(matches!(notif2, Incoming::Publish(Publish { .. })));

    let notif3 = eventloop2.poll().await.unwrap();
    assert!(matches!(notif3, Incoming::Publish(Publish { .. })));

    let (client, mut eventloop) = common::get_client(config.clone());

    let notif1 = eventloop.poll().await.unwrap(); // connack
    assert_eq!(
        notif1,
        Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        })
    );

    client
        .publish(qos0topic, QoS::AtMostOnce, true, "")
        .await
        .unwrap();

    client
        .publish(qos1topic, QoS::AtLeastOnce, true, "")
        .await
        .unwrap();

    client
        .publish(qos2topic, QoS::ExactlyOnce, true, "")
        .await
        .unwrap();

    let _ = eventloop.poll().await.unwrap(); // incoming: puback
    let _ = eventloop.poll().await.unwrap(); // incoming: pubrec
    let _ = eventloop.poll().await.unwrap(); // incoming: pubcomp

    let notif2 = eventloop.poll().await.unwrap();

    // We cleared all the retained messages so should only receive pings
    assert_eq!(notif2, Incoming::PingResp);
    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Retained message test Successful".green().to_string());
}

// TODO: messages not being retained
pub async fn test_retained_messages(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Retained message test".yellow().to_string());

    let mut config = MqttOptions::new(
        "conformance-retained-message",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = common::get_client(config.clone());

    let qos0topic = "fromb/qos 0";
    let qos1topic = "fromb/qos 1";
    let qos2topic = "fromb/qos 2";
    let wildcardtopic = "fromb/+";

    let notification1 = eventloop.poll().await.unwrap(); // connack
    assert_eq!(
        notification1,
        Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        })
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
        .publish(qos2topic, QoS::ExactlyOnce, true, "QoS::ExactlyOnce")
        .await
        .unwrap();

    let _ = eventloop.poll().await.unwrap(); // incoming: puback
    let _ = eventloop.poll().await.unwrap(); // incoming: pubrec
    let _ = eventloop.poll().await.unwrap(); // incoming: pubcomp

    client
        .subscribe(wildcardtopic, QoS::ExactlyOnce)
        .await
        .unwrap();

    let _ = eventloop.poll().await.unwrap(); //suback

    let notif1 = eventloop.poll().await.unwrap();
    assert!(matches!(notif1, Incoming::Publish(Publish { .. })));

    let notif2 = eventloop.poll().await.unwrap();
    assert!(matches!(notif2, Incoming::Publish(Publish { .. })));

    let notif3 = eventloop.poll().await.unwrap();
    assert!(matches!(notif3, Incoming::Publish(Publish { .. })));

    drop(client);
    drop(eventloop);

    let (client, mut eventloop) = common::get_client(config.clone());

    let notif1 = eventloop.poll().await.unwrap(); // connack
    assert_eq!(
        notif1,
        Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        })
    );

    client
        .publish(qos0topic, QoS::AtMostOnce, true, "")
        .await
        .unwrap();

    client
        .publish(qos1topic, QoS::AtLeastOnce, true, "")
        .await
        .unwrap();

    client
        .publish(qos2topic, QoS::ExactlyOnce, true, "QoS::ExactlyOnce")
        .await
        .unwrap();

    let _ = eventloop.poll().await.unwrap(); // incoming: puback
    let _ = eventloop.poll().await.unwrap(); // incoming: pubrec
    let _ = eventloop.poll().await.unwrap(); // incoming: pubcomp

    let notif2 = eventloop.poll().await.unwrap();

    // We cleared all the retained messages so should only receive pings
    assert_eq!(notif2, Incoming::PingResp);
    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Retained message test Successful".green().to_string());
}

// TODO: Currently rumqttc panics for this test. According to spec broker should be the one handling this not client
pub async fn test_zero_length_clientid(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Zero length clientid".yellow().to_string());
    let mut config = MqttOptions::new("", &conformance_config.server, conformance_config.port);
    config.set_keep_alive(Duration::from_secs(5));
    config.set_clean_session(true);

    let (_client, mut eventloop) = common::get_client(config);

    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        })
    );

    let mut config2 = MqttOptions::new("", &conformance_config.server, conformance_config.port);
    config2.set_keep_alive(Duration::from_secs(5));
    config2.set_clean_session(false);

    let (_client, mut eventloop) = common::get_client(config2);

    let notification1 = eventloop.poll().await.unwrap(); // connack
    assert_eq!(
        notification1,
        Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::BadClientId
        })
    );
}

pub async fn test_offline_message_queueing(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Offline message Queue test".yellow().to_string());
    let mut config1 = MqttOptions::new(
        "conformance-offline-message-queue",
        &conformance_config.server,
        conformance_config.port,
    );
    config1.set_keep_alive(Duration::from_secs(5));
    config1.set_clean_session(false);

    let (client1, mut eventloop1) = common::get_client(config1.clone());
    let _ = eventloop1.poll().await.unwrap(); // connack

    client1.subscribe("+/+", QoS::AtLeastOnce).await.unwrap();
    let _ = eventloop1.poll().await.unwrap(); // suback

    client1.disconnect().await.unwrap();
    let _ = eventloop1.poll().await; // disconnect

    let mut config2 = MqttOptions::new(
        "conformance-offline-message-queue2",
        &conformance_config.server,
        conformance_config.port,
    );
    config2.set_keep_alive(Duration::from_secs(5));
    config2.set_clean_session(true);

    let (client2, mut eventloop2) = common::get_client(config2);
    let _ = eventloop2.poll().await.unwrap(); // connack

    client2
        .publish("topic/a", QoS::AtMostOnce, true, "QoS::AtMostOnce")
        .await
        .unwrap();

    client2
        .publish("topic/b", QoS::AtLeastOnce, true, "QoS::AtLeastOnce")
        .await
        .unwrap();

    let _ = eventloop2.poll().await.unwrap(); // incoming: puback

    client2.disconnect().await.unwrap();
    let _ = eventloop2.poll().await;

    let (_client1, mut eventloop1) = common::get_client(config1.clone());
    let notif1 = eventloop1.poll().await.unwrap(); // connack

    assert_eq!(
        notif1,
        Incoming::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success
        })
    );

    // NOTE: We store QoS0 publish's when client is not connected.
    let notif1 = eventloop1.poll().await.unwrap(); // QoS0 publish
    let notif2 = eventloop1.poll().await.unwrap(); // QoS1 publish
    let notif1_is_pubilsh = matches!(notif1, Incoming::Publish(Publish { .. }));
    let notif2_is_pubilsh = matches!(notif2, Incoming::Publish(Publish { .. }));

    match (notif1_is_pubilsh, notif2_is_pubilsh) {
        (true, false) => {
            PROGRESS_BAR.println(
                "Brokers doesn't queue's QoS0 messages for offline clients."
                    .green()
                    .to_string(),
            );
        }
        (true, true) => {
            PROGRESS_BAR.println(
                "Brokers queue's QoS0 messages for offline clients."
                    .green()
                    .to_string(),
            );
        }
        _ => {
            panic!("First notif should be a publish");
        }
    }

    client2
        .publish("topic/a", QoS::AtLeastOnce, true, "")
        .await
        .unwrap();
    let _ = eventloop2.poll().await.unwrap(); // incoming: puback

    client2
        .publish("topic/a", QoS::AtLeastOnce, true, "")
        .await
        .unwrap();
    let _ = eventloop2.poll().await.unwrap(); // incoming: puback

    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Offline message Queue test successful".green().to_string());
}

pub async fn test_will_message(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Will message test".yellow().to_string());
    let mut config = MqttOptions::new(
        "conformance-will-message",
        &conformance_config.server,
        conformance_config.port,
    );
    config
        .set_clean_session(true)
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
        Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        })
    );

    let mut config2 = MqttOptions::new(
        "conformance-will-message2",
        &conformance_config.server,
        conformance_config.port,
    );
    config2.set_keep_alive(Duration::from_secs(5));

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

    assert!(matches!(notif3, Incoming::Publish(Publish { .. })));
    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Will message test Successful".green().to_string());
}

pub async fn test_dollar_topic_filter(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Dollar topic test".yellow().to_string());
    let mut config = MqttOptions::new(
        "conformance-dollar-topic-filter",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));

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
    assert!(matches!(notif1, Incoming::PingResp));
    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Dollar topic test Successful".green().to_string());
}

pub async fn test_unsubscribe(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Unsubscribe test".yellow().to_string());
    let mut config = MqttOptions::new(
        "conformance-unsubscribe",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));

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
    let notif1 = eventloop1.poll().await.unwrap(); // unsuback

    assert!(matches!(notif1, Incoming::UnsubAck(UnsubAck { .. })));

    let mut config = MqttOptions::new(
        "conformance-unsubscribe2",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));

    let (client2, mut eventloop2) = common::get_client(config.clone());
    let _ = eventloop2.poll().await.unwrap(); // connack

    client2
        .publish("topicA", QoS::AtLeastOnce, false, "")
        .await
        .unwrap();
    let _ = eventloop2.poll().await.unwrap(); // puback
    client2
        .publish("topicA/B", QoS::AtLeastOnce, false, "")
        .await
        .unwrap();
    let _ = eventloop2.poll().await.unwrap(); // puback
    client2
        .publish("topicC", QoS::AtLeastOnce, false, "")
        .await
        .unwrap();
    let _ = eventloop2.poll().await.unwrap(); // puback

    let notif1 = eventloop1.poll().await.unwrap();
    assert!(matches!(notif1, Incoming::Publish(Publish { .. })));

    let notif2 = eventloop1.poll().await.unwrap();
    assert!(matches!(notif2, Incoming::Publish(Publish { .. })));

    for _ in 0..5 {
        let notif3 = eventloop1.poll().await.unwrap();
        // dbg!(&notif3);
        assert!(matches!(notif3, Incoming::PingResp));
    }
    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Unsubscribe test Successful".green().to_string());
}

pub async fn test_subscribe_failure(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Subscribe failure test".yellow().to_string());
    let mut config = MqttOptions::new(
        "conformance-sub-failure",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = common::get_client(config);

    let _ = eventloop.poll().await.unwrap(); // connack

    client
        .subscribe("$SYS/rumqttd/donotsubscribe", QoS::AtMostOnce)
        .await
        .unwrap();

    // TODO: Err should contain more descriptive message
    assert!(eventloop.poll().await.is_err());
    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Subscribe failure test Successful".green().to_string());
}

// TODO: re-eval this after retransmission is implemented in broker
pub async fn test_redelivery_on_reconnect(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Redelivery test".yellow().to_string());
    let mut config = MqttOptions::new(
        "conformance-test-redelivery",
        &conformance_config.server,
        conformance_config.port,
    );
    config
        .set_keep_alive(Duration::from_secs(5))
        .set_clean_session(false);

    let (client, mut eventloop) = common::get_client(config.clone());
    let _ = eventloop.poll().await.unwrap(); // connack

    client.subscribe("+/+", QoS::AtLeastOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // suback

    drop(eventloop);

    let mut config2 = MqttOptions::new(
        "conformance-test-redelivery2",
        &conformance_config.server,
        conformance_config.port,
    );
    config2.set_keep_alive(Duration::from_secs(5));

    let (client2, mut eventloop2) = common::get_client(config2);
    let _ = eventloop2.poll().await.unwrap(); // connack

    // Qos 1 Publish
    client2
            .publish("topic/a", QoS::AtLeastOnce, false, "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
            .await
            .unwrap();

    let _ = eventloop2.poll().await.unwrap(); // puback

    // drop(eventloop);

    let (_, mut eventloop) = common::get_client(config);
    let _ = eventloop.poll().await.unwrap(); // connack

    let incoming1 = eventloop.poll().await.unwrap(); // incoming:publish
    assert!(matches!(incoming1, Incoming::Publish(Publish { .. })));

    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println("Redelivery test Successful".green().to_string());
}

pub async fn test_connack_with_clean_session(conformance_config: &ConformanceConfig) {
    PROGRESS_BAR.set_message("Connack with clean session test".yellow().to_string());
    // To make sure any of the previous tests doesn't affect this create a connection and drop it
    // immediately to clean any previous state
    let mut config = MqttOptions::new(
        "conformance-connack-clean",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = common::get_client(config);
    client.subscribe("topic/a", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // connack
    drop(client);
    drop(eventloop);

    // Make a connection with clean_session false to create any random state
    let mut config = MqttOptions::new(
        "conformance-connack-clean",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));
    config.set_clean_session(false);
    let (client, mut eventloop) = common::get_client(config);
    let _ = eventloop.poll().await.unwrap(); // connack
    client.subscribe("topic/a", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // suback
    drop(client);
    drop(eventloop);

    // Should have a session present
    let mut config = MqttOptions::new(
        "conformance-connack-clean",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));
    config.set_clean_session(false);
    let (client, mut eventloop) = common::get_client(config);

    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Packet::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success
        })
    );
    drop(client);
    drop(eventloop);

    // Should drop previous_state if any and reply with session_present false
    let mut config = MqttOptions::new(
        "conformance-connack-clean",
        &conformance_config.server,
        conformance_config.port,
    );
    config.set_keep_alive(Duration::from_secs(5));

    let (_client, mut eventloop) = common::get_client(config);

    let notification1 = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        notification1,
        Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success
        })
    );
    PROGRESS_BAR.inc(1);
    PROGRESS_BAR.println(
        "Connack with clean session test Successful"
            .green()
            .to_string(),
    );
}
