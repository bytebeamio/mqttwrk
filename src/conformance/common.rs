use rumqttc::{ConnectionError, Event, EventLoop, Incoming, MqttOptions};
use std::time::Duration;

pub fn mqtt_config() -> MqttOptions {
    let mut mqttoptions = MqttOptions::new("rumqtt-sync", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_clean_session(true);
    mqttoptions
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

    pub async fn poll(&mut self) -> Result<Event, ConnectionError> {
        loop {
            let tmp = self.inner.poll().await?;
            match tmp {
                Event::Outgoing(_) => continue,
                Event::Incoming(_) => return Ok(tmp),
            }
        }
    }
}

pub struct WrappedEvent {
    inner: Event,
    pub curr_condition: bool,
}

impl WrappedEvent {
    pub fn new(event: Event) -> Self {
        WrappedEvent {
            inner: event,
            curr_condition: true,
        }
    }

    pub fn is_publish(self) -> Self {
        if self.curr_condition == false {
            return self;
        }

        if let Event::Incoming(Incoming::Publish(_)) = &self.inner {
            WrappedEvent {
                inner: self.inner,
                curr_condition: true,
            }
        } else {
            WrappedEvent {
                inner: self.inner,
                curr_condition: false,
            }
        }
    }

    pub fn evaluate(self) -> bool {
        self.curr_condition
    }
}
