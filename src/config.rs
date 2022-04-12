use std::env;

pub struct AppConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topic: String,
    pub redis_dsn: String
}

impl AppConfig {
    pub fn new() -> Self {
        AppConfig {
            bootstrap_servers: env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned()),
            group_id: env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "shopping-cart".to_owned()),
            topic: env::var("KAFKA_TOPIC").unwrap_or_else(|_| "cart-events".to_owned()),
            redis_dsn: env::var("REDIS_DSN").unwrap_or_else(
                |_| "redis://:eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81@127.0.0.1:6379/1".to_owned()
            )
        }
    }
}