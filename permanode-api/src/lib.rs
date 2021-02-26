pub mod add_feed_source;
pub mod application;
pub mod config;
pub mod listener;
pub mod notifications;

#[macro_use]
extern crate rocket;

use async_trait::async_trait;
use chronicle::*;
pub use config::ApiConfig;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
