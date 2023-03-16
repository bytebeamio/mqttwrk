use crate::DataType;
use crate::RunnerConfig;
use crate::SimulatorConfig;

impl From<SimulatorConfig> for RunnerConfig {
    fn from(value: SimulatorConfig) -> Self {
        match value.data_type {
            DataType::Default(_) => {
                panic!("Shouldn't specify payload size when specifying data_type")
            }
            _ => {}
        };

        Self {
            server: value.server,
            port: value.port,
            publishers: value.publishers,
            subscribers: value.subscribers,
            publish_qos: value.publish_qos,
            subscribe_qos: value.subscribe_qos,
            count: value.count,
            rate: value.rate,
            payload: value.data_type,
            topic_format: value.topic_format,
            disable_unqiue_clientid_prefix: value.disable_unique_clientid_prefix,
            keep_alive: value.keep_alive,
            max_inflight: value.max_inflight,
            conn_timeout: value.conn_timeout,
            ca_file: value.ca_file,
            show_pub_stat: value.show_pub_stat,
            show_sub_stat: value.show_sub_stat,
            sleep_sub: value.sleep_sub,
        }
    }
}
