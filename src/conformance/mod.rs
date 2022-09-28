mod basic;
mod common;

use basic::*;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub async fn start() {
    // test_basic().await;
    // test_keepalive().await;
    // session_test().await;
    // test_will_message().await;
    // test_connack_with_clean_session().await;
    // test_offline_message_queueing().await;
    // test_subscribe_failure().await;
    test_redelivery_on_reconnect().await;
    // test_overlapping_subscriptions().await;
    // test_retained_messages().await;
    // test_retain_on_different_connect().await;
    // test_unsubscribe().await;

    // NOTE: Client cannot publish to $ topics
    // test_dollar_topic_filter().await;

    // NOTE: rumqttc don't allow empty clientID with clean_session false
    // test_zero_length_clientid().await;
}
