use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct KafkaMessage {
    pub key:       Vec<u8>,
    pub payload:   Vec<u8>,
    pub timestamp: i64,
    pub headers:   HashMap<String, String>,
}
