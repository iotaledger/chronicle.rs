// Scale of these modules are from up to down
#[macro_use]
pub mod launcher;
pub mod engine;
pub mod dashboard;
pub mod ring;
pub mod cluster;
pub mod node;
pub mod stage;
pub mod worker;
pub mod connection;
pub mod api;
#[macro_use]
extern crate cdrs_helpers_derive;
#[macro_use]
extern crate smallbox;
