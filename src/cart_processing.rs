use crate::datamodel::{Event, CartState};
use std::collections::HashMap;
use redis::RedisResult;
use crate::config::AppConfig;
use crate::utils::get_redis_connection;
use log::warn;


pub struct ProcessorResults {
    pub to_delete: Vec<CartState>,
    pub to_update: Vec<CartState>
}

pub struct CartProcessor {
    pub redis: redis::Connection
}

impl CartProcessor {
    pub fn new(cfg: &AppConfig) -> Self {
        match get_redis_connection(cfg) {
            Ok(connection) => CartProcessor{ redis: connection },
            Err(er) => panic!("{}", er)
        }
    }

    pub fn order_key(order_number: &str, store_id: u32) -> String{
        format!("{};{}", order_number, store_id)
    }

    fn get_carts(&mut self, cart_keys: Vec<(String, u32)>) -> HashMap<(String, u32), CartState> {
        let redis_keys = cart_keys.iter()
            .map(|(order, store)| Self::order_key(order, *store)).collect::<Vec<String>>();

        let serialized_carts: RedisResult<Vec<Option<String>>> = redis::cmd("MGET").arg(
            redis_keys
        ).query(&mut self.redis);
        match serialized_carts {
            Err(er) => panic!("{}", er),
            Ok(carts) => {
                cart_keys.into_iter().zip(carts)
                    .filter(|(_, v)| v.is_some())
                    .fold(HashMap::new(), |mut acc, (key, raw_value)| {
                        let parsed_cart  = serde_json::from_str(&raw_value.unwrap());
                        if let Ok(cart_state) = parsed_cart {
                            acc.insert(key, cart_state);
                        } else {
                            warn!("JSON parse error");
                        }
                        acc
                    })
            }
        }
    }

    pub fn del_carts(&mut self, cart_keys: &Vec<CartState>) -> RedisResult<()>{
        let mut pipe = redis::pipe();
        cart_keys.iter()
            .map(|state| Self::order_key(&state.order_number, state.store_id))
            .for_each(|key|{
                pipe.unlink(key);
            });
        pipe.query(&mut self.redis)?;
        Ok(())
    }

    pub fn set_carts(&mut self, cart_keys: &Vec<CartState>) -> RedisResult<()> {
        let mut pipe = redis::pipe();
        cart_keys.iter()
            .map(|state| Self::order_key(&state.order_number, state.store_id))
            .zip(cart_keys).for_each(|(key, state)|{
                pipe.set(key, serde_json::to_string(state).unwrap());
            });
        pipe.query(&mut self.redis)?;
        Ok(())
    }

    pub fn process_events(&mut self, msgs: Vec<Event>) -> ProcessorResults {
        let cart_events: HashMap<(String, u32), Vec<Event>> = msgs.into_iter()
            .filter(|event| event.order_number.is_some() && event.store_id.is_some())
            .fold(HashMap::new(), |mut acc, event| {
                let entry = acc.entry(
                    (event.order_number.clone().unwrap(), event.store_id.unwrap())
                ).or_insert_with(|| Vec::new());
                entry.push(event);
                acc
            });

        let cart_keys = cart_events.keys()
            .map(|(order, store)| (order.clone(), *store))
            .collect::<Vec<(String, u32)>>();

        let mut redis_carts = self.get_carts(cart_keys);

        for (order_key, events) in cart_events {
            let cart = redis_carts.entry(order_key.clone()).or_insert(
                CartState::new(&order_key.0, order_key.1)
            );
            events.into_iter().for_each(move |ev| cart.accept(ev));
        } 
        let to_update: Vec<CartState> = redis_carts.values().filter(|x| !x.is_empty()).cloned().collect();
        let to_delete: Vec<CartState> = redis_carts.values().filter(|x| x.is_empty()).cloned().collect();

        match self.del_carts(&to_delete) {
            Ok(_) => (),
            Err(e) => warn!("{}", e)
        };
        match self.set_carts(&to_update) {
            Ok(()) => (),
            Err(e) => warn!("{}", e)
        };

        ProcessorResults {
            to_update,
            to_delete
        }
    }
}