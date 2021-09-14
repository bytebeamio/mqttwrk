use std::{fs, io, sync::Arc};

use futures::StreamExt;
use rumqttc::{MqttOptions, QoS, Transport, Key};
use tokio::task;

use crate::BenchConfig;

mod publisher;
mod subscriber;

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("IO error = {0:?}")]
    Io(#[from] io::Error),
    #[error("Connection error = {0:?}")]
    Connection(#[from] rumqttc::ConnectionError),
    #[error("Wrong packet = {0:?}")]
    WrongPacket(rumqttc::Incoming),
    #[error("Client error = {0:?}")]
    Client(#[from] rumqttc::ClientError),
}

pub(crate) async fn start(config: BenchConfig) {
    let config = Arc::new(config);
    let mut handles = futures::stream::FuturesUnordered::new();

    // spawning subscribers
    for i in 0..config.subscribers {
        let config = Arc::clone(&config);
        let id = format!("rumqtt_sub-{:05}", i);
        let mut subscriber = subscriber::Subscriber::new(id, config).await.unwrap();
        handles.push(task::spawn(async move {
            subscriber.start().await;
        }));
    }

    // spawing publishers
    for i in 0..config.publishers {
        let config = Arc::clone(&config);
        let id = format!("rumqtt_pub-{:05}", i);
        let mut publisher = publisher::Publisher::new(id, config).await.unwrap();
        handles.push(task::spawn(async move {
            publisher.start().await;
        }));
    }

    // await and consume all futures
    while handles.next().await.is_some() {}
}

pub(crate) fn options(config: Arc<BenchConfig>, id: &str) -> io::Result<MqttOptions> {
    let mut options = MqttOptions::new(id, &config.server, config.port);
    options.set_keep_alive(config.keep_alive);
    options.set_inflight(config.max_inflight);
    options.set_connection_timeout(config.conn_timeout);

    if let Some(ca_file) = &config.ca_file {
        let ca = fs::read(ca_file)?;
        let client_auth = match &config.client_cert {
            Some(f) => {
                let cert = fs::read(f)?;
                let key = fs::read(&config.client_key.as_ref().unwrap())?;
                Some((cert, Key::RSA(key)))
            }
            None => None,
        };

        options.set_transport(Transport::tls(ca, client_auth, None));
    }

    Ok(options)
}

/// get QoS level. Default is AtLeastOnce.
fn get_qos(qos: i16) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::AtLeastOnce,
    }
}
