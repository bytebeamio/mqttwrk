use rumqttc::{AsyncClient, ConnectionError, Event, EventLoop, Incoming, MqttOptions};

pub fn get_client(config: MqttOptions) -> (AsyncClient, WrappedEventLoop) {
    let (client, eventloop) = AsyncClient::new(config, 10);
    let weventloop = WrappedEventLoop::new(eventloop);
    (client, weventloop)
}

pub struct WrappedEventLoop {
    inner: EventLoop,
}

// This WrappedEventLoop is used to ignore `Event::Outgoing` type of events as
// we don't need it during testing
impl WrappedEventLoop {
    pub fn new(eventloop: EventLoop) -> Self {
        WrappedEventLoop { inner: eventloop }
    }

    pub async fn poll(&mut self) -> Result<Incoming, ConnectionError> {
        loop {
            let tmp = self.inner.poll().await?;
            match tmp {
                Event::Outgoing(_) => continue,
                Event::Incoming(v) => return Ok(v),
            }
        }
    }
}
