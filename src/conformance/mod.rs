mod basic;

use std::time::Duration;

use basic::*;
use indicatif::ProgressBar;
use once_cell::sync::Lazy;

use crate::common::PROGRESS_STYLE;
use crate::ConformanceConfig;

pub static PROGRESS_BAR: Lazy<indicatif::ProgressBar> = Lazy::new(|| {
    let progress_bar = ProgressBar::new(12)
        .with_prefix("Conformance test:")
        .with_style((*PROGRESS_STYLE).clone());

    progress_bar.enable_steady_tick(Duration::from_secs_f32(0.1));
    progress_bar
});

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub async fn start(config: ConformanceConfig) {
    test_basic(&config).await;
    test_keepalive(&config).await;
    session_test(&config).await;
    test_will_message(&config).await;
    test_connack_with_clean_session(&config).await;
    test_offline_message_queueing(&config).await;
    test_subscribe_failure(&config).await;
    test_redelivery_on_reconnect(&config).await;
    test_overlapping_subscriptions(&config).await;
    test_retained_messages(&config).await;
    test_retain_on_different_connect(&config).await;
    test_unsubscribe(&config).await;

    // NOTE: Client cannot publish to $ topics
    // test_dollar_topic_filter().await;

    // NOTE: rumqttc don't allow empty clientID with clean_session false
    // test_zero_length_clientid().await;
    PROGRESS_BAR.finish_and_clear();
}
