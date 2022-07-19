use rumqttc::{Client, Connection, MqttOptions};
use std::time::Duration;

pub fn mqtt_config() -> MqttOptions {
    let mut mqttoptions = MqttOptions::new("rumqtt-sync", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions
}

pub fn mqtt_client(config: MqttOptions) -> (Client, Connection) {
    Client::new(config, 10)
}
