#[macro_use]
extern crate criterion;
use chronicle::bundle::Bvalidator;
use chronicle::subscriber::Subscriber;
use chronicle::transaction::Tvalidator;
use criterion::Criterion;
use futures::channel::mpsc;
use futures::executor::block_on;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::thread;

fn mpmc() {
    let (t, r) = crossbeam_channel::unbounded();
    let (t2, _) = mpsc::channel(1);
    let sub = Subscriber::new(t);
    let val = Tvalidator::new(r, t2);
    let sub_2 = sub.clone();
    let val_2 = val.clone();

    sub.tx.send("Hello World!").unwrap();
    sub_2.tx.send("Chronicle start Scribing!").unwrap();
    assert_eq!(val_2.rx.recv(), Ok("Hello World!"));
    assert_eq!(val.rx.recv(), Ok("Chronicle start Scribing!"));
}

fn mpsc_multi_thread() {
    const AMT: u32 = 100;
    const NTHREADS: u32 = 8;
    let (_, r) = crossbeam_channel::unbounded();
    let (t2, r2) = mpsc::channel(0);
    let txn = Tvalidator::new(r, t2);
    let bundle = Bvalidator::new(r2);

    let t = thread::spawn(move || {
        let result: Vec<_> = block_on(bundle.rx.collect());
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

fn bench_chronicle(c: &mut Criterion) {
    c.bench_function("mpmc", |b| b.iter(|| mpmc()));
    c.bench_function("mpsc_multi_thread", |b| b.iter(|| mpsc_multi_thread()));
}

criterion_group!(benches, bench_chronicle);
criterion_main!(benches);
