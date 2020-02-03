use std::convert::Infallible;
use serde::{Deserialize, Serialize};
use warp::Filter;
use bundle::Hash;

use crate::cql::{StorageBackend, Connection};

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ReqBody {
    command: String,
    hashes: String,
}

pub fn router(
    session: impl StorageBackend + Connection
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
    .and(warp::body::content_length_limit(1024 * 16))
    .and(warp::body::json())
    .and(warp::any().map(move || session.clone()))
    .and_then(select_trytes)
}

async fn select_trytes(
    command: ReqBody,
    session: impl StorageBackend
) -> Result<impl warp::Reply, Infallible> {
    if let Ok(tx) = session.select_transaction(&Hash::from_str(&command.hashes)).await {
        return Ok(tx.address().to_string());
    }
    Ok(String::new())
}
