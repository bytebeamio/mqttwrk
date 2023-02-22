mod basic;

use std::time::Duration;

use basic::*;
use indicatif::ProgressBar;

use crate::common::PROGRESS_STYLE;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub async fn start() {
    // tokio::task::spawn(async move {
    let progress_bar = ProgressBar::new(12)
        .with_prefix("Conformance test:")
        .with_style((*PROGRESS_STYLE).clone());
    // });

    progress_bar.enable_steady_tick(Duration::from_secs_f32(0.1));

    test_basic(&progress_bar).await;

    test_keepalive(&progress_bar).await;
    session_test(&progress_bar).await;
    test_will_message(&progress_bar).await;
    test_connack_with_clean_session(&progress_bar).await;
    test_offline_message_queueing(&progress_bar).await;
    test_subscribe_failure(&progress_bar).await;
    test_redelivery_on_reconnect(&progress_bar).await;
    test_overlapping_subscriptions(&progress_bar).await;
    test_retained_messages(&progress_bar).await;
    test_retain_on_different_connect(&progress_bar).await;
    test_unsubscribe(&progress_bar).await;

    // NOTE: Client cannot publish to $ topics
    // test_dollar_topic_filter().await;

    // NOTE: rumqttc don't allow empty clientID with clean_session false
    // test_zero_length_clientid().await;
}
