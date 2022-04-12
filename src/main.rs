mod utils;
mod config;
mod cart_processing;
mod datamodel;

use rdkafka::Message;
use rdkafka::consumer::Consumer;
use serde_json::Result;
use log::{info, warn};
use crate::utils::get_kafka_consumer;
use crate::config::AppConfig;
use crate::datamodel::Event;
use crate::cart_processing::CartProcessor;

fn main() {
    env_logger::init();
    let cfg = AppConfig::new();
    let mut cart_processor = CartProcessor::new(&cfg);
    info!("Connected to Redis");

    let kafka_consumer = get_kafka_consumer(&cfg).expect("failed to connect to kafka");
    kafka_consumer.subscribe(&[cfg.topic.as_str()]).expect("failed to connect to topic");
    info!("Connected to broker: {}, topic: {}", cfg.bootstrap_servers, cfg.topic);

    loop {
        for msg in kafka_consumer.iter() {
            match msg {
                Ok(bmsg) => {
                    if let Some(p) = bmsg.payload() {
                        let parse_res: Result<Event>= serde_json::from_slice(p);
                        match parse_res {
                            Ok(event) => {
                                info!("received message: {}", serde_json::to_string(&event).unwrap());
                                let results = cart_processor.process_events(vec![event]);

                                info!("{} carts to update", results.to_update.len());
                                info!("{} carts to delete", results.to_delete.len());
                            },
                            Err(er) => {warn!("{}", er);}
                        }
                    } else { info!("No payload in message"); }
                }
                Err(ker) => warn!("failed to consume message: {}", ker)
            }
        }
    }
}
