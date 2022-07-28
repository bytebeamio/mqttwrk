mod basic;
mod common;

use basic::*;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub(crate) async fn start() {
    // session_test().await;
    // test_basic().await;
    // test_retained_messages().await;
    // test_will_message().await;
    // test_connack_with_clean_session().await;
    // test_offline_message_queueing().await;
    // test_subscribe_failure().await;
    // test_dollar_topic_filter().await;

    // TODO: We don't yet support unsubscribe
    // test_unsubscribe().await;

    // NOTE: rumqttc don't allow empty clientID with clean_session false
    // test_zero_length_clientid().await;

    // test_overlapping_subscriptions().await;
}
