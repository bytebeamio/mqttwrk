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
