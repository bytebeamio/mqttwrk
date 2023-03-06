use indicatif::ProgressStyle;
use once_cell::sync::Lazy;
use rumqttc::{AsyncClient, ConnectionError, Event, EventLoop, Incoming, MqttOptions};

pub static PROGRESS_STYLE: Lazy<indicatif::ProgressStyle> = Lazy::new(|| {
    ProgressStyle::with_template(
        "{spinner:.bold.bright.yellow} {prefix:>22} {pos:>7}/{len:7} {bar:40.cyan/blue} {msg}",
    )
    .expect("progress style template should be correct")
    .progress_chars("##-")
});

pub enum Stats {
    PubStats(PubStats),
    SubStats(SubStats),
}

#[derive(Default, Debug)]
pub struct SubStats {
    pub publish_count: u64,
    pub puback_count: u64,
    pub reconnects: u64,
    pub throughput: f32,
}

#[derive(Default, Debug)]
pub struct PubStats {
    pub outgoing_publish: u64,
    pub throughput: f32,
    pub reconnects: u64,
}

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
                Event::Outgoing(_) | Event::Incoming(Incoming::PingResp) => continue,
                Event::Incoming(v) => return Ok(v),
            }
        }
    }
}
