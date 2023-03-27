use crate::cli::DataEvent;
use fake::{Dummy, Fake, Faker};
use futures::Stream;
use futures::StreamExt;
use serde::Serialize;
use std::collections::VecDeque;
use std::task::Poll;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_util::time::DelayQueue;

#[derive(Debug, Serialize, Dummy)]
pub struct Imu {
    sequence: u32,
    timestamp: u64,
    #[dummy(faker = "1.0 .. 2.8")]
    ax: f64,
    #[dummy(faker = "1.0 .. 2.8")]
    ay: f64,
    #[dummy(faker = "9.79 .. 9.82")]
    az: f64,
    #[dummy(faker = "0.8 .. 1.0")]
    pitch: f64,
    #[dummy(faker = "0.8 .. 1.0")]
    roll: f64,
    #[dummy(faker = "0.8 .. 1.0")]
    yaw: f64,
    #[dummy(faker = "-45.0 .. -15.0")]
    magx: f64,
    #[dummy(faker = "-45.0 .. -15.0")]
    magy: f64,
    #[dummy(faker = "-45.0 .. -15.0")]
    magz: f64,
}

#[derive(Debug, Serialize, Dummy)]
pub struct Gps {
    sequence: u32,
    timestamp: u64,
    latitude: f64,
    longitude: f64,
}

#[derive(Debug, Serialize, Dummy)]
pub struct Bms {
    sequence: u32,
    timestamp: u64,
    #[dummy(faker = "250")]
    periodicity_ms: i32,
    #[dummy(faker = "40.0 .. 45.0")]
    mosfet_temperature: f64,
    #[dummy(faker = "35.0 .. 40.0")]
    ambient_temperature: f64,
    #[dummy(faker = "1")]
    mosfet_status: i32,
    #[dummy(faker = "16")]
    cell_voltage_count: i32,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_1: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_2: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_3: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_4: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_5: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_6: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_7: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_8: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_9: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_10: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_11: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_12: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_13: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_14: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_15: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_16: f64,
    #[dummy(faker = "8")]
    cell_thermistor_count: i32,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_1: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_2: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_3: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_4: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_5: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_6: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_7: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_8: f64,
    #[dummy(faker = "1")]
    cell_balancing_status: i32,
    #[dummy(faker = "95.0 .. 96.0")]
    pack_voltage: f64,
    #[dummy(faker = "15.0 .. 20.0")]
    pack_current: f64,
    #[dummy(faker = "80.0 .. 90.0")]
    pack_soc: f64,
    #[dummy(faker = "9.5 .. 9.9")]
    pack_soh: f64,
    #[dummy(faker = "9.5 .. 9.9")]
    pack_sop: f64,
    #[dummy(faker = "100 .. 150")]
    pack_cycle_count: i64,
    #[dummy(faker = "2000 .. 3000")]
    pack_available_energy: i64,
    #[dummy(faker = "2000 .. 3000")]
    pack_consumed_energy: i64,
    #[dummy(faker = "0")]
    pack_fault: i32,
    #[dummy(faker = "1")]
    pack_status: i32,
}

pub fn generate_data(data_type: DataEvent) -> String {
    let payload: String = match data_type {
        DataEvent::Default { payload_size, .. } => "\0".repeat(payload_size),
        DataEvent::Gps { sequence, .. } => {
            let fake_data = vec![dummy_gps(sequence as u32)];
            serde_json::to_string(&fake_data).unwrap()
        }
        DataEvent::Imu { sequence, .. } => {
            let fake_data = vec![dummy_imu(sequence as u32)];
            serde_json::to_string(&fake_data).unwrap()
        }
        DataEvent::Bms { sequence, .. } => {
            let fake_data = vec![dummy_bms(sequence as u32)];
            serde_json::to_string(&fake_data).unwrap()
        }
    };

    payload
}

pub fn dummy_imu(sequence: u32) -> Imu {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    Imu {
        sequence,
        timestamp,
        ..Faker.fake()
    }
}

pub fn dummy_bms(sequence: u32) -> Bms {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    Bms {
        sequence,
        timestamp,
        ..Faker.fake()
    }
}

pub fn dummy_gps(sequence: u32) -> Gps {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    Gps {
        sequence,
        timestamp,
        ..Faker.fake()
    }
}

pub struct GenData {
    count: usize,
    events: VecDeque<DataEvent>,
}

impl GenData {
    pub fn new(count: usize, events: VecDeque<DataEvent>) -> Self {
        Self { count, events }
    }
}

pub struct St {
    queue: _Inner<DataEvent>,
    count: usize,
}

impl GenData {
    pub fn into_stream(self) -> St {
        // If all the DataEvent have Duration of `0` that means we don't want to throttle so use
        // `VecDeque` instead of `DelayQueue`
        if !self.events.iter().all(|event| event.duration().is_zero()) {
            let mut delay_queue = DelayQueue::new();
            for event in &self.events {
                delay_queue.insert(*event, event.duration());
            }

            St {
                queue: _Inner::DelayQueue(delay_queue),
                count: self.count,
            }
        } else {
            let mut queue = VecDeque::new();
            for event in &self.events {
                queue.push_back(*event);
            }
            St {
                queue: _Inner::Queue(queue),
                count: self.count,
            }
        }
    }
}

pub enum _Inner<DataEvent> {
    DelayQueue(DelayQueue<DataEvent>),
    Queue(VecDeque<DataEvent>),
}

impl Stream for St {
    type Item = (DataEvent, String);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.count == 0 {
            return Poll::Ready(None);
        }

        let event = match &mut self.queue {
            _Inner::DelayQueue(ref mut q) => match q.poll_next_unpin(cx) {
                Poll::Ready(Some(t)) => {
                    let event = t.into_inner();
                    q.insert(event.inc_sequence(), event.duration());
                    event
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            },
            _Inner::Queue(ref mut q) => {
                let event = q.pop_front().unwrap();
                q.push_back(event.inc_sequence());
                event
            }
        };

        self.count -= 1;

        Poll::Ready(Some((event, generate_data(event))))
    }
}
