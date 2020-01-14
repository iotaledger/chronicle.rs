// TODO: This is temporary lint, remove it once lib.rs has implementation
#![allow(unused_imports)]

use futures::channel::mpsc;

pub mod bundle;
pub mod subscriber;
pub mod transaction;

use bundle::Bvalidator;
use subscriber::Subscriber;
use transaction::Tvalidator;

extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
extern crate r2d2;
extern crate time;
mod db;
mod psedo_bundle;
use psedo_bundle::PseudoBundle;

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

    /*
    #[test]
    fn scylladb_access() {
        fn connect_to_db() -> db::CurrentSession {
            let mut session = db::create_db_session().expect("create db session error");
            db::create_keyspace(&mut session).expect("create keyspace error");
            db::create_bundle_table(&mut session).expect("create keyspace error");
            session
        }

        println!("connecting to db");
        let mut session = connect_to_db();
        println!("adding a psudo bundle");
        let test_bundle = PseudoBundle {
            bundle: String::from("Psedo Bundle 1"),
            time: time::Timespec::new(10001, 0),
            info: String::from("Info of Psedo Bundle 1"),
        };
        db::add_bundle(&mut session, test_bundle).expect("add bundle error");
        let prepared_query = db::prepare_add_bundle(&mut session).expect("prepare query error");
        db::execute_add_bundle(
            &mut session,
            &prepared_query,
            PseudoBundle {
                bundle: String::from("Psedo Bundle 2"),
                time: time::Timespec::new(10002, 0),
                info: String::from("Info of Psedo Bundle 2"),
            },
        )
        .expect("execute add bundle error");
        let bundles = db::select_bundles_by_time_range(
            &mut session,
            time::Timespec::new(10000, 0),
            time::Timespec::new(10010, 0),
        )
        .expect("select bundles error");
        // assert_eq!("Psedo Bundle 2", bundles[0].bundle);
        // assert_eq!("Psedo Bundle 1", bundles[1].bundle);
    }
    */
}
