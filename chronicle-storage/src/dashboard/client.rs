use futures::{StreamExt,SinkExt};
use tokio_tungstenite::connect_async;
use url::Url;
use tokio_tungstenite::tungstenite::Message;
use super::websocket::SocketMsg;

pub async fn add_nodes(ws: &str,addresses: Vec<String>, uniform_rf: u8) -> bool {
    let request = Url::parse(ws).unwrap();
    // connect to dashboard
    match connect_async(request).await {
        Ok((mut ws_stream, _)) => {
            // add scylla nodes
            for address in addresses {
                // add node
                let msg = SocketMsg::AddNode(address);
                let j = serde_json::to_string(&msg).expect("invalid address format");
                let m = Message::text(j);
                ws_stream.send(m).await.unwrap();
                // await till the node is added
                if let Some(msg) = ws_stream.next().await {
                    let event: SocketMsg = serde_json::from_str(msg.unwrap().to_text().unwrap()).unwrap();
                    if let SocketMsg::Ok(_) = event {
                    } else {
                        ws_stream.close(None).await.unwrap();
                        return false
                    }
                } else {
                    println!("unable to reach the websocket server");
                    ws_stream.close(None).await.unwrap();
                    return false
                };
            }
            // build the ring
            let msg = SocketMsg::TryBuild(uniform_rf);
            let j = serde_json::to_string(&msg).unwrap();
            let m = Message::text(j);
            ws_stream.send(m).await.unwrap();
            // close socket and return true.
            ws_stream.close(None).await.unwrap();
            true
        }
        Err(_) => {
            false
        }
    }
}
