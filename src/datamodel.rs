use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Product {
    pub product_sku: String
}

impl Product {
    pub fn new(sku: &str) -> Self {
        Product {
            product_sku: String::from(sku)
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct User {
    pub uuid: String
}

impl User {
    pub fn new(uuid: &str) -> Self {
        User {
            uuid: String::from(uuid)   
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Event {
    pub name: String,
    pub quantity: Option<u32>,
    pub store_id: Option<u32>,
    pub order_number: Option<String>,
    pub timestamp: u64,
    pub product: Option<Product>,
    pub user: Option<User>
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CartState {
    pub user_uuid: Option<String>,
    pub order_number: String,
    pub store_id: u32,
    pub ts: Option<u64>,
    pub products: HashMap<String, u32>
}

impl CartState {
    pub fn new(order_number: &str, store_id: u32) -> Self {
        CartState {
            user_uuid: None,
            order_number: String::from(order_number),
            store_id,
            ts: None,
            products: HashMap::new()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.products.is_empty()
    }

    pub fn accept(&mut self, event: Event) {
        if event.name == "cart_deleted" {
            return
        }
        if event.store_id.is_none() || event.order_number.is_none() {
            return
        }

        if self.user_uuid.is_none() && event.user.is_some() {
            self.user_uuid = Some(event.user.unwrap().uuid);
        }

        let sku = event.product.unwrap().product_sku;
        self.ts = Some(event.timestamp);

        if let Some(quantity) = event.quantity {
            self.products.insert(sku, quantity);
        } else {
            self.products.remove(&sku);
        }
    }
}