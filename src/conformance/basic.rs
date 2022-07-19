#![allow(unused_imports)]

use crate::conformance::common::{mqtt_client, mqtt_config};
use colored::Colorize;
use rumqttc::{Client, MqttOptions, QoS};
use std::thread;
use std::time::Duration;

// TODO: Connecting to same socket twice should fail

#[test]
pub fn basic() {
    println!("{}", "Basic test starting".yellow());
    let config = mqtt_config();
    // let (client, connection) = mqtt_client(config.clone());
    // drop(client);
    // drop(connection);

    let (mut client, mut connection) = Client::new(config, 10);

    client.subscribe("topic/a", QoS::ExactlyOnce).unwrap();

    for (i, notification) in connection.iter().enumerate() {
        println!("id: {:?}, Notification = {:?}", i, notification);
    }

    client
        .publish("topic/a", QoS::AtMostOnce, false, "QoS::AtMostOnce")
        .unwrap();
    // client
    //     .publish("topic/a", QoS::AtLeastOnce, false, "QoS::AtLeastOnce")
    //     .unwrap();
    // client
    //     .publish("topic/a", QoS::ExactlyOnce, false, "QoS::ExactlyOnce")
    //     .unwrap();

    println!("{}", "Basic test succedeed".green())
}
