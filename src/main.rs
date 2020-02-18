// mod collector;
mod router;
mod statements;
mod storage;

// Scale of these module is from up to down
#[macro_use]
pub mod engine;
pub mod cluster;
pub mod node;
pub mod stage;

#[macro_use]
extern crate cdrs_helpers_derive;

use crate::storage::{CQLSession, Connection};
use std::env;
use warp::Filter;

#[tokio::main]
async fn main() {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "chronicle=info");
    }
    pretty_env_logger::init();

    let session = CQLSession::establish_connection("0.0.0.0:9042")
        .await
        .expect("Storage connection failed");

    let routes = router::post(session).with(warp::log("chronicle"));

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}
