//! Tool to vet mqtt brokers. The goal here is to test and benchmark
//! mqtt brokers for features, robustness, performance and scalability
//!
//! Goals of the test suite
//!
//! - Spawn n clients with publish and subscribe on the same topic (and report thoughput and latencies)
//! - Spawn n clinets with publishes and 1 subscription to pull all the data (used to simulate a sink in the cloud)

#[macro_use]
extern crate log;
#[macro_use]
extern crate colour;

mod bench;
mod cli;
mod common;
mod conformance;
mod round;
mod test;

use crate::cli::Cli;
use clap::Parser;

fn main() {
    console_subscriber::init();

    pretty_env_logger::init();
    let config: Cli = Cli::parse();

    match config {
        Cli::Bench(config) => {
            bench::start(config);
        }
        Cli::Simulator(config) => {
            bench::start(config);
        }
        Cli::Round(config) => {
            round::start(config).unwrap();
        }
        Cli::Conformance(config) => {
            conformance::start(config);
        }
        Cli::Test => {
            test::start();
        }
    }
}
