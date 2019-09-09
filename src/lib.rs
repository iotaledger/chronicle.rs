// TODO: This is temporary lint, remove it once lib.rs has implementation
#![allow(unused_imports)]

use futures::channel::mpsc;

pub mod bundle;
pub mod subscriber;
pub mod transaction;

use bundle::Bvalidator;
use subscriber::Subscriber;
use transaction::Tvalidator;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use std::thread;

    #[test]
    fn test_mpmc() {
        let (t, r) = crossbeam_channel::unbounded();
        let (t2, _) = mpsc::channel(1);
        let sub = Subscriber::new(t);
        let val = Tvalidator::new(r, t2);
        let sub2 = sub.clone();
        let val2 = val.clone();

        sub.tx.send("Hello World!").unwrap();
        sub2.tx.send("Chronicle start Scribing!").unwrap();

        assert_eq!(val2.rx.recv(), Ok("Hello World!"));
        assert_eq!(val.rx.recv(), Ok("Chronicle start Scribing!"));
    }

    #[test]
    fn test_mpsc_multi_thread() {
        const AMT: u32 = 10000;
        const NTHREADS: u32 = 8;
        let (_, r) = crossbeam_channel::unbounded();
        let (t2, r2) = mpsc::channel(0);
        let txn = Tvalidator::new(r, t2);
        let bundle = Bvalidator::new(r2);

        let t = thread::spawn(move || {
            let result: Vec<_> = block_on(bundle.rx.collect());
            assert_eq!(result.len(), (AMT * NTHREADS) as usize);
            for item in result {
                assert_eq!(item, 1);
            }
        });

        for _ in 0..NTHREADS {
            let mut txn = txn.clone();

            thread::spawn(move || {
                for _ in 0..AMT {
                    block_on(txn.tx.send(1)).unwrap();
                }
            });
        }

        drop(txn);
        t.join().ok().unwrap();
    }
}
