use std::convert::TryFrom;

/// A websocket command sent from the dashboard
#[repr(u8)]
#[derive(Debug)]
pub enum WsCommand {
    /// Register a topic
    Register = 0,
    /// Unregister a topic
    Unregister = 1,
}

impl TryFrom<u8> for WsCommand {
    type Error = u8;

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0 => Ok(WsCommand::Register),
            1 => Ok(WsCommand::Unregister),
            _ => Err(val),
        }
    }
}
