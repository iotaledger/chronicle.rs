use futures::{StreamExt,SinkExt};
use tokio_tungstenite::connect_async;
use url::Url;
use tokio_tungstenite::tungstenite::Message;
use super::websocket::SocketMsg;

pub async fn add_nodes(ws: &str,addresses: Vec<String>, uniform_rf: u8) -> Result<(), ()> {
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
                        return Err(())
                    }
                } else {
                    println!("unable to reach the websocket server");
                    ws_stream.close(None).await.unwrap();
                    return Err(())
                };
            }
            // build the ring
            let msg = SocketMsg::TryBuild(uniform_rf);
            let j = serde_json::to_string(&msg).unwrap();
            let m = Message::text(j);
            ws_stream.send(m).await.unwrap();
            // await till the ring is built
            if let Some(msg) = ws_stream.next().await {
                if let SocketMsg::BuiltRing(true) = serde_json::from_str(msg.unwrap().to_text().unwrap()).unwrap(){
                } else {
                    unreachable!("add nodes fn");
                };
            };
            // close socket and return true.
            ws_stream.close(None).await.unwrap();
            Ok(())
        }
        Err(_) => {
            Err(())
        }
    }
}
