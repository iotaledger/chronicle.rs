use serde::{
    Deserialize,
    Serialize,
};

#[derive(Deserialize, Serialize, Clone)]
pub enum SocketMsg<T> {
    General(T),
    API(T),
    Broker(T),
    Scylla(T),
}

impl<T: Serialize> SocketMsg<T> {
    pub fn to_outgoing(&self) -> Result<String, String> {
        match self {
            SocketMsg::General(v) => Err("No outgoing message for general commands".to_owned()),
            SocketMsg::API(v) => {
                todo!()
            }
            SocketMsg::Broker(v) => {
                serde_json::to_string(&permanode_broker::application::SocketMsg::PermanodeBroker(v))
                    .map_err(|e| e.to_string())
            }
            SocketMsg::Scylla(v) => {
                serde_json::to_string(&scylla::application::SocketMsg::Scylla(v)).map_err(|e| e.to_string())
            }
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub enum ConfigCommand {
    Rollback,
}
