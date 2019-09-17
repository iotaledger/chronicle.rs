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

static NTHREADS: i32 = 100;

fn mpmc() {
    let (t, r) = crossbeam_channel::unbounded();
    let (t2, _) = mpsc::channel(1);
    let sub = Subscriber::new(t);
    let val = Tvalidator::new(r, t2);
    let mut children = Vec::new();
    let mut vals = Vec::new();

    for id in 0..NTHREADS {
        let sub_k = sub.clone();
        let val_k = val.clone();
        let child = thread::spawn(move || {
            sub_k.tx.send(id).unwrap();
        });
        children.push(child);
        vals.push(val_k);
    }
    let mut ids = Vec::with_capacity(NTHREADS as usize);
    for i in 0..NTHREADS {
        ids.push(vals[i as usize].rx.recv());
    }
    for child in children {
        child.join().expect("oops! the child thread panicked");
    }
}

fn mpsc_multi_thread() {
    const AMT: u32 = 1;
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

fn bench_mpmc(c: &mut Criterion) {
    c.bench_function("mpmc", |b| b.iter(|| mpmc()));
}

fn bench_mpsc_multi_thread(c: &mut Criterion) {
    c.bench_function("mpsc_multi_thread", |b| b.iter(|| mpsc_multi_thread()));
}

criterion_group!(benches, bench_mpmc, bench_mpsc_multi_thread);
criterion_main!(benches);
