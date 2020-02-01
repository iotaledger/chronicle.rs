#[macro_use]
extern crate cdrs_helpers_derive;

use std::env;
use warp::Filter;

pub mod cql;
mod router;
mod statements;

#[tokio::main]
async fn main() {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "chronicle=info");
    }
    pretty_env_logger::init();

    let api = warp::any().map(|| "Hello, World!");

    let routes = api.with(warp::log("chronicle"));
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}
