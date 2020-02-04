use std::convert::Infallible;
use serde::{Deserialize, Serialize};
use warp::Filter;
use bundle::Hash;

use crate::storage::{StorageBackend, Connection, EdgeKind};

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ReqBody {
    command: String,
    hashes: Option<Vec<String>>,
    bundle: Option<Vec<String>>,
    address: Option<Vec<String>>,
    tag: Option<Vec<String>>,
    approvee: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ResTrytes(Vec<String>);

pub fn post(
    session: impl StorageBackend + Connection
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
    .and(warp::body::content_length_limit(1024 * 16))
    .and(warp::body::json())
    .and(warp::any().map(move || session.clone()))
    .and_then(body_filter)
}

async fn body_filter(
    req: ReqBody,
    session: impl StorageBackend
) -> Result<warp::reply::Json, Infallible> {

    match &req.command[..] {
        "getTrytes" => {
            if let Some(hashes) = req.hashes {
                return get_trytes(hashes, session).await;
            }
        },
        "findTransactions" => {
            return find_transactions(req, session).await;
        },
        _ => ()
    }

    Ok(warp::reply::json(&""))
}

async fn get_trytes(
    hashes: Vec<String>,
    session: impl StorageBackend
) -> Result<warp::reply::Json, Infallible> {
    let mut res = ResTrytes(Vec::new());

    for hash in hashes.iter() {
        if let Ok(tx) = session.select_transaction(&Hash::from_str(&hash)).await {
            // TODO: transaction model for serde
            res.0.push(tx.bundle().to_string());
        }
    }

    Ok(warp::reply::json(&res))
}

async fn find_transactions(
    req: ReqBody,
    session: impl StorageBackend
) -> Result<warp::reply::Json, Infallible> {
    let mut res = ResTrytes(Vec::new());

    if let Some(bundles) = req.bundle {
        for bundle in bundles.iter() {
            if let Ok(hashes) = session.select_transaction_hashes(&Hash::from_str(&bundle), EdgeKind::Bundle).await {
                // TODO: transaction model for serde
                hashes.iter().for_each(|hash|(res.0.push(hash.to_string())));
            }
        }
    } else if let Some(addresses) = req.address {
        for address in addresses.iter() {
            if let Ok(hashes) = session.select_transaction_hashes(&Hash::from_str(&address), EdgeKind::Address).await {
                // TODO: transaction model for serde
                hashes.iter().for_each(|hash|(res.0.push(hash.to_string())));
            }
        }
    } else if let Some(tags) = req.tag {
        for tag in tags.iter() {
            if let Ok(hashes) = session.select_transaction_hashes(&Hash::from_str(&tag), EdgeKind::Tag).await {
                // TODO: transaction model for serde
                hashes.iter().for_each(|hash|(res.0.push(hash.to_string())));
            }
        }
    } else if let Some(approvees) = req.approvee {
        for approvee in approvees.iter() {
            if let Ok(hashes) = session.select_transaction_hashes(&Hash::from_str(&approvee), EdgeKind::Approvee).await {
                // TODO: transaction model for serde
                hashes.iter().for_each(|hash|(res.0.push(hash.to_string())));
            }
        }
    }

    Ok(warp::reply::json(&res))
}