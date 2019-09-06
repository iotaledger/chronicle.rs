use futures::channel::mpsc;
use futures::executor::block_on;
use futures::stream::StreamExt;
use futures::sink::SinkExt;
use std::thread;


#[derive(Debug)]
pub struct Bvalidator<T> {
    pub rx: mpsc::Receiver<T>,
}

impl<T> Bvalidator<T> {
    pub fn new(r: mpsc::Receiver<T>) -> Bvalidator<T> {
        Bvalidator { rx: r }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence() {
        let (tx, rx) = mpsc::channel(1);
        let val = Bvalidator::new(rx);

        let amt = 20;
        let t = thread::spawn(move || {
            block_on(send_sequence(amt, tx))
        });
        let list: Vec<_> = block_on(val.rx.collect());
        let mut list = list.into_iter();
        for i in (1..amt + 1).rev() {
            assert_eq!(list.next(), Some(i));
        }
        assert_eq!(list.next(), None);

        t.join().unwrap();
    }

    async fn send_sequence(n: u8, mut sender: mpsc::Sender<u8>) {
        for x in 0..n {
            sender.send(n - x).await.unwrap();
        }
    }
}
