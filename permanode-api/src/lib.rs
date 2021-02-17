#![feature(decl_macro)]
pub mod add_feed_source;
pub mod application;
pub mod listener;
pub mod notifications;

#[macro_use]
extern crate rocket;

use async_trait::async_trait;
use chronicle::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
