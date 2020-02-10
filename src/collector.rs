use tmq::{subscribe, Context, Result};
use tokio::stream::StreamExt;

async fn sn_subscribe() -> Result<()> {
    // TODO: Subscribe sn_trytes event
    let mut sub = subscribe(&Context::new())
        .connect("tcp://nodes.iota.cafe:5556")?
        .subscribe(b"tx_trytes")?;

    if let Some(msg) = sub.next().await {
        let message = msg?
            .iter()
            .map(|hash| hash.as_str().unwrap_or("invalid text").to_string())
            .collect::<Vec<String>>();
        dbg!(message);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscribe() -> Result<()> {
        sn_subscribe().await
    }
}
