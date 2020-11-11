use rumqttc::{AsyncClient, EventLoop, MqttOptions};
use std::{fs, io};
use hdrhistogram::Histogram;
use rumqttc::*;

pub enum Status{
    Increment(i32),
    Hist(Histogram<u64>)
}


pub struct Link {
    pub id: String,
    pub client: AsyncClient,
    pub eventloop: EventLoop,
    pub sender: Sender<Status>,
}

impl Link {
    pub(crate) fn new(
        id: &str,
        server: &str,
        port: u16,
        keep_alive: u16,
        max_inflight: u16,
        connection_timeout: u64,
        ca_file: Option<String>,
        client_cert_file: Option<String>,
        client_key_file: Option<String>,
        sender: Sender<Status>,

    ) -> io::Result<Link> {
        let mut mqttoptions = MqttOptions::new(id, server, port);
        mqttoptions.set_keep_alive(keep_alive);
        mqttoptions.set_inflight(max_inflight);
        mqttoptions.set_connection_timeout(connection_timeout);
        mqttoptions.set_max_request_batch(10);

        if let Some(ca_file) = ca_file {
            mqttoptions.set_ca(fs::read(ca_file)?);
        }

        if let Some(client_cert_file) = client_cert_file {
            let cert = fs::read(client_cert_file)?;
            let key = fs::read(client_key_file.as_ref().unwrap())?;
            mqttoptions.set_client_auth(cert, key);
        }

        let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
        Ok(Link {
            id: id.to_owned(),
            client,
            eventloop,
            sender,
        })
    }
}
