mod basic;
mod common;

use basic::*;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub(crate) async fn start() {
    test_basic().await;
    // test_retained_messages().await;
}
