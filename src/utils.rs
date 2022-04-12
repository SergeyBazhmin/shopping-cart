use redis::{RedisResult, Client, Connection};
use rdkafka::{ClientConfig, consumer::{BaseConsumer}, error::KafkaResult};
use crate::config::AppConfig;


pub fn get_redis_connection(cfg: &AppConfig) -> RedisResult<Connection> {
    let client = Client::open(cfg.redis_dsn.as_str())?;
    let con = client.get_connection()?;
    Ok(con)
}


pub fn get_kafka_consumer(cfg: &AppConfig) -> KafkaResult<BaseConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", cfg.bootstrap_servers.as_str())
        .set("group.id", cfg.group_id.as_str())
        .create()
}
