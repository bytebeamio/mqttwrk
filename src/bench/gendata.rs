use crate::cli::DataType;
use fake::{Dummy, Fake, Faker};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

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

pub fn generate_data(sequence: usize, data_type: DataType) -> String {
    let payload: String = match data_type {
        DataType::Default(payload_size) => {
            let fake_data = vec![0; payload_size];
            serde_json::to_string(&fake_data).unwrap()
        }
        DataType::Gps => {
            let fake_data = vec![dummy_gps(sequence as u32)];
            serde_json::to_string(&fake_data).unwrap()
        }
        DataType::Imu => {
            let fake_data = vec![dummy_imu(sequence as u32)];
            serde_json::to_string(&fake_data).unwrap()
        }
        DataType::Bms => {
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
