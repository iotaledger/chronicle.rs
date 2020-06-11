use chronicle_common::actor;
use super::supervisor::{
    Peer,
    Topic,
};
use tokio::sync::mpsc;
use chronicle_storage::worker::Error;
use super::supervisor::Sender;
use async_zmq::{Result, StreamExt, subscribe::Subscribe, errors::RecvError};

actor!(ZmqBuilder { peer: Peer, supervisor_tx: Sender });

impl ZmqBuilder {
    pub fn build(self) -> Zmq {
        Zmq {
            peer: self.peer.unwrap(),
            supervisor_tx: self.supervisor_tx.unwrap(),
        }
    }
}

pub struct Zmq {
    peer: Peer,
    supervisor_tx: Sender
}
pub struct ZmqId;

pub enum Event {
    Void { pid: Box<ZmqId> },
    Error { kind: Error, pid: Box<ZmqId> },
}

impl Zmq {
    pub async fn run(mut self) {
        // create channel
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        if let Ok(mut zmq) = self.init() {
            // start recv msgs from zmq topic according the subscribed topic
            match self.peer.get_topic() {
                // this topic used to store newly seen transactions
                Topic::Trytes => {
                    while let Some(msgs) = zmq.next().await {
                        if let Ok(msgs) = msgs {
                            for msg in msgs {
                                // process trytes msg

                            }
                        } else if let Err(RecvError::Interrupted) = msgs {
                            // we assume is retryable
                            continue;
                        } else {
                            unreachable!("unexepcted error: bug {:?}", msgs);                }
                    }
                }
                // this topic used to store confirmed transactions only
                Topic::SnTrytes => {
                    while let Some(msgs) = zmq.next().await {
                        if let Ok(msgs) = msgs {
                            for msg in msgs {
                                // process sn_trytes msg

                            }
                        } else if let Err(RecvError::Interrupted) = msgs {
                            // we assume is retryable
                            continue;
                        } else {
                            unreachable!("unexepcted error: bug {:?}", msgs);                }
                    }
                }
                // this topic used to upsert milestone column in transaction table (confirmed status)
                Topic::Sn => {
                    while let Some(msgs) = zmq.next().await {
                        if let Ok(msgs) = msgs {
                            for msg in msgs {
                                // process sn msg
                            }
                        } else if let Err(RecvError::Interrupted) = msgs {
                            // we assume is retryable
                            continue;
                        } else {
                            unreachable!("unexepcted error: bug {:?}", msgs);                }
                    }
                }
            }
        } else {
            // tell supervisor by returning peer with connected = false.
            todo!()
        }
    }

    fn init(&mut self) -> Result<Subscribe> {
        let zmq = async_zmq::subscribe(self.peer.get_address())?.connect()?;
        zmq.set_subscribe(&self.peer.get_topic_as_string())?;
        Ok(zmq)
    }

    fn handle_trytes() {
        unimplemented!()
    }
    fn handle_sn_trytes() {
        unimplemented!()
    }
    fn handle_sn() {
        unimplemented!()
    }
}
