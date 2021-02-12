pub mod add_feed_source;
pub mod application;
pub mod notifications;

use async_trait::async_trait;
use chronicle::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
