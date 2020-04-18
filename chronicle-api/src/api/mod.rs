macro_rules! response {
    (body: $body:expr) => {
        hyper::Response::builder()
            .header("Content-Type", "application/json")
            .body(Body::from($body))
            .unwrap()
    };
    (status: $status:tt, body: $body:expr) => {
        hyper::Response::builder()
            .header("Content-Type", "application/json")
            .status(hyper::StatusCode::$status)
            .body(Body::from($body))
            .unwrap()
    };
}

pub mod api;
pub mod endpoint;
pub mod gettrytes;
pub mod router;
