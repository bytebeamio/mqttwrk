#![allow(unused_imports)]

use crate::conformance::common::{self, WrappedEventLoop};
use colored::Colorize;
use rumqttc::{
    matches, AsyncClient, ConnAck, ConnectReturnCode, Event, Incoming, LastWill, MqttOptions,
    Outgoing, Packet, PubAck, Publish, QoS, SubAck, Subscribe, SubscribeFilter,
    SubscribeReasonCode, UnsubAck,
};
use std::thread;
use std::time::Duration;

// TODO?: Connecting to same socket twice should fail
pub async fn test_basic() {
    yellow_ln!("Basic test");
    let mut config = MqttOptions::new("conformance-basic", "localhost", 1883);
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

    #[cfg(feature = "qos2")]
    {
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
    }

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

    let incoming = eventloop.poll().await.unwrap(); // incoming:publish
    assert!(matches!(incoming, Incoming::PubAck(PubAck { .. })));

    let incoming = eventloop.poll().await.unwrap(); // incoming:publish
    assert!(matches!(incoming, Incoming::Publish(Publish { .. })));

    // Qos 2 Publish
    #[cfg(feature = "qos2")]
    {
        client
            .publish("topic/q2", QoS::ExactlyOnce, false, "QoS::ExactlyOnce")
            .await
            .unwrap();

        let incoming = eventloop.poll().await.unwrap(); // incoming:publish
        assert!(matches!(incoming, Incoming::PubAck(PubAck { .. })));

        let incoming = eventloop.poll().await.unwrap(); // incoming:publish
        assert!(matches!(incoming, Incoming::Publish(Publish { .. })));
    }

    green_ln!("Basic test succedeed")
}

pub async fn session_test() {
    yellow_ln!("Starting session test");

    let mut config = MqttOptions::new("conformance-session", "localhost", 1883);
    config.set_keep_alive(Duration::from_secs(5));
    config.set_clean_session(true);

    let (client, mut eventloop) = common::get_client(config.clone());
    let incoming = eventloop.poll().await.unwrap(); // connack
    assert!(matches!(incoming, Incoming::ConnAck(ConnAck { .. })));

    client.disconnect().await.unwrap();

    let mut config = MqttOptions::new("conformance-session", "localhost", 1883);
    config.set_keep_alive(Duration::from_secs(5));
    config.set_clean_session(false);

    let (client, mut eventloop) = common::get_client(config.clone());
    let incoming = eventloop.poll().await.unwrap(); // connack

    assert_eq!(
        incoming,
        Packet::ConnAck(ConnAck {
            session_present: false,
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
            session_present: true,
            code: ConnectReturnCode::Success
        })
    );
    green_ln!("Session test successful");
}

pub async fn test_overlapping_subscriptions() {
    yellow_ln!("Overlapping subscriptions test");
    let mut config = MqttOptions::new("conformance-overlapping-subscriptions", "localhost", 1883);
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
    dbg!(&notif1, &notif2);

    let notif1_is_publish = matches!(notif1, Incoming::Publish(Publish { .. }));
    let notif2_is_publish = matches!(notif2, Incoming::Publish(Publish { .. }));

    match (notif1_is_publish, notif2_is_publish) {
        (true, false) | (false, true) => {
            yellow_ln!("Broker publishes 1 message per overlapping subscription");
        }
        (true, true) => {
            yellow_ln!("Broker publishes 1 message for all matching subscription")
        }
        (false, false) => {
            panic!("Should receive atleast 1 publish message");
        }
    }
    green_ln!("Overlapping subscriptions test Successful");
}

// TODO: Not disconnecting the client if keep_alive time has passed with no messages from client
pub async fn test_keepalive() {
    yellow_ln!("Ping test");
    let mut config = MqttOptions::new("conformance-overlapping-subscriptions", "localhost", 1883);
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
        green_ln!("Ping response {} received", i);
    }

    green_ln!("Ping test successful");
}

pub async fn test_retain_on_different_connect() {
    yellow_ln!("Retained message test");

    let mut config = MqttOptions::new("conformance-retained-message", "localhost", 1883);
    config.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = common::get_client(config.clone());

    let qos0topic = "fromb/qos 0";
    let qos1topic = "fromb/qos 1";
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

    let _ = eventloop.poll().await.unwrap(); // incoming: puback

    #[cfg(feature = "qos2")]
    {
        let qos2topic = "fromb/qos2";
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
    assert!(matches!(notif1, Incoming::Publish(Publish { .. })));

    let notif2 = eventloop.poll().await.unwrap();
    assert!(matches!(notif2, Incoming::Publish(Publish { .. })));

    #[cfg(feature = "qos2")]
    {
        let notif3 = eventloop.poll().await.unwrap();
        assert!(matches!(notif3, Incoming::Publish(Publish { .. })));
    }

    drop(client);
    drop(eventloop);

    let mut config2 = MqttOptions::new("conformance-retained-message2", "localhost", 1883);
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
    assert_eq!(notif2, Incoming::PingResp);
    green_ln!("Retained message test Successful");
}

// TODO: messages not being retained
pub async fn test_retained_messages() {
    yellow_ln!("Retained message test");

    let mut config = MqttOptions::new("conformance-retained-message", "localhost", 1883);
    config.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = common::get_client(config.clone());

    let qos0topic = "fromb/qos 0";
    let qos1topic = "fromb/qos 1";
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

    let _ = eventloop.poll().await.unwrap(); // incoming: puback

    #[cfg(feature = "qos2")]
    {
        let qos2topic = "fromb/qos2";
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
    assert!(matches!(notif1, Incoming::Publish(Publish { .. })));

    let notif2 = eventloop.poll().await.unwrap();
    assert!(matches!(notif2, Incoming::Publish(Publish { .. })));

    #[cfg(feature = "qos2")]
    {
        let notif3 = eventloop.poll().await.unwrap();
        assert!(matches!(notif3, Incoming::Publish(Publish { .. })));
    }

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
    assert_eq!(notif2, Incoming::PingResp);
    green_ln!("Retained message test Successful");
}

// TODO: Currently rumqttc panics for this test. According to spec broker should be the one handling this not client
pub async fn test_zero_length_clientid() {
    let mut config = MqttOptions::new("", "localhost", 1883);
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

    let mut config2 = MqttOptions::new("", "localhost", 1883);
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

pub async fn test_offline_message_queueing() {
    yellow_ln!("Offline message Queue test");
    let mut config1 = MqttOptions::new("conformance-offline-message-queue", "localhost", 1883);
    config1.set_keep_alive(Duration::from_secs(5));
    config1.set_clean_session(false);

    let (client1, mut eventloop1) = common::get_client(config1.clone());
    let _ = eventloop1.poll().await.unwrap(); // connack

    client1.subscribe("+/+", QoS::AtLeastOnce).await.unwrap();
    let _ = eventloop1.poll().await.unwrap(); // suback

    client1.disconnect().await.unwrap();
    let _ = eventloop1.poll().await; // disconnect

    let mut config2 = MqttOptions::new("conformance-offline-message-queue2", "localhost", 1883);
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
            yellow_ln!("Brokers doesn't queue's QoS0 messages for offline clients.");
        }
        (true, true) => {
            yellow_ln!("Brokers queue's QoS0 messages for offline clients.");
        }
        _ => {
            panic!("First notif should be a publish");
        }
    }
    #[cfg(feature = "qos2")]
    {
        let _notif3 = eventloop1.poll().await.unwrap(); // QoS2 publish
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

    green_ln!("Offline message Queue test successful");
}

pub async fn test_will_message() {
    yellow_ln!("Will message test");
    let mut config = MqttOptions::new("conformance-will-message", "localhost", 1883);
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

    let mut config2 = MqttOptions::new("conformance-will-message2", "localhost", 1883);
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
    green_ln!("Will message test Successful");
}

pub async fn test_dollar_topic_filter() {
    yellow_ln!("Dollar topic test");
    let mut config = MqttOptions::new("conformance-dollar-topic-filter", "localhost", 1883);
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
    green_ln!("Dollar topic test Successful");
}

pub async fn test_unsubscribe() {
    yellow_ln!("Unsubscribe test");
    let mut config = MqttOptions::new("conformance-unsubscribe", "localhost", 1883);
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

    let mut config = MqttOptions::new("conformance-unsubscribe2", "localhost", 1883);
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
        dbg!(&notif3);
        assert!(matches!(notif3, Incoming::PingResp));
    }
    green_ln!("Unsubscribe test Successful");
}

pub async fn test_subscribe_failure() {
    yellow_ln!("Subscribe failure test");
    let mut config = MqttOptions::new("conformance-sub-failure", "localhost", 1883);
    config.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = common::get_client(config);

    let _ = eventloop.poll().await.unwrap(); // connack

    client
        .subscribe("test/shouldfail", QoS::AtMostOnce)
        .await
        .unwrap();

    // TODO: Err should contain more descriptive message
    assert!(eventloop.poll().await.is_err());
    green_ln!("Subscribe failure test Successful");
}

// TODO: re-eval this after retransmission is implemented in broker
pub async fn test_redelivery_on_reconnect() {
    yellow_ln!("Redelivery test");
    let mut config = MqttOptions::new("conformance-test-redelivery", "localhost", 1883);
    config
        .set_keep_alive(Duration::from_secs(5))
        .set_clean_session(false);

    let (client, mut eventloop) = common::get_client(config.clone());
    let _ = eventloop.poll().await.unwrap(); // connack

    client.subscribe("+/+", QoS::AtLeastOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // suback

    drop(eventloop);

    let mut config2 = MqttOptions::new("conformance-test-redelivery2", "localhost", 1883);
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

    #[cfg(feature = "qos2")]
    {
        // Qos 2 Publish
        client2
            .publish("topic/a", QoS::ExactlyOnce, false, "QoS::ExactlyOnce")
            .await
            .unwrap();
    }

    let (_, mut eventloop) = common::get_client(config);
    let _ = eventloop.poll().await.unwrap(); // connack

    let incoming1 = eventloop.poll().await.unwrap(); // incoming:publish
    dbg!(&incoming1);
    assert!(matches!(incoming1, Incoming::Publish(Publish { .. })));

    #[cfg(feature = "qos2")]
    {
        let incoming2 = eventloop.poll().await.unwrap(); // incoming:publish
        assert!(matches!(incoming2, Incoming::Publish(Publish { .. })));
    }
    green_ln!("Redelivery test Successful");
}

pub async fn test_connack_with_clean_session() {
    yellow_ln!("Connack with clean session test");
    // To make sure any of the previous tests doesn't affect this create a connection and drop it
    // immediately to clean any previous state
    let mut config = MqttOptions::new("conformance-connack-clean", "localhost", 1883);
    config.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = common::get_client(config);
    client.subscribe("topic/a", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // connack
    drop(client);
    drop(eventloop);

    // Make a connection with clean_session false to create any random state
    let mut config = MqttOptions::new("conformance-connack-clean", "localhost", 1883);
    config.set_keep_alive(Duration::from_secs(5));
    config.set_clean_session(false);
    let (client, mut eventloop) = common::get_client(config);
    let _ = eventloop.poll().await.unwrap(); // connack
    client.subscribe("topic/a", QoS::AtMostOnce).await.unwrap();
    let _ = eventloop.poll().await.unwrap(); // suback
    drop(client);
    drop(eventloop);

    // Should have a session present
    let mut config = MqttOptions::new("conformance-connack-clean", "localhost", 1883);
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
    let mut config = MqttOptions::new("conformance-connack-clean", "localhost", 1883);
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
    green_ln!("Connack with clean session test Successful");
}
