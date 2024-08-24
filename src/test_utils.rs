use core::time;
use std::{
    collections::HashMap,
    fmt::{self, Formatter},
    sync::Arc,
    time::{Instant, SystemTime},
};

use futures::{future::BoxFuture, FutureExt};
use tokio::sync::Mutex;

pub struct TestValue {
    id: i32,
    duration: u64,
}

impl TestValue {
    pub fn new(id: i32, duration: u64) -> Self {
        Self { id, duration }
    }
}

#[derive(Clone, Copy)]
pub enum FutureTiming {
    Polled(SystemTime),
    Completed(SystemTime, SystemTime),
}

#[derive(Clone)]
pub struct FutureTimings(Arc<Mutex<HashMap<i32, FutureTiming>>>);

impl FutureTimings {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    pub async fn set_polled(&self, id: i32) {
        self.0
            .lock()
            .await
            .insert(id, FutureTiming::Polled(SystemTime::now()));
    }

    pub async fn set_completed(&self, id: i32) {
        let mut this = self.0.lock().await;

        let polled_entry = this.remove(&id).unwrap();

        let completed = match polled_entry {
            FutureTiming::Polled(start) => FutureTiming::Completed(start, SystemTime::now()),
            FutureTiming::Completed(_, _) => panic!("Future already completed"),
        };

        this.insert(id, completed);
    }

    pub async fn run_after(&self, id1: i32, id2: i32) -> bool {
        let timings = self.0.lock().await;

        let p1 = timings.get(&id1).unwrap();
        let p2 = timings.get(&id2).unwrap();

        match (p1, p2) {
            (FutureTiming::Polled(start1), FutureTiming::Completed(_, end2))
            | (FutureTiming::Completed(start1, _), FutureTiming::Completed(_, end2)) => {
                start1 > end2
            }
            _ => false,
        }
    }

    pub async fn run_in_parallel(&self, id1: i32, id2: i32) -> bool {
        let timings = self.0.lock().await;

        let p1 = timings.get(&id1).unwrap();
        let p2 = timings.get(&id2).unwrap();

        match (p1, p2) {
            (FutureTiming::Completed(start1, end1), FutureTiming::Completed(start2, end2)) => {
                start1 < end2 && start2 < end1
            }
            _ => false,
        }
    }

    pub fn get_tracked_fn(&self) -> impl Fn(TestValue) -> BoxFuture<'static, i32> {
        let this = self.clone();

        move |value| {
            let processed = this.clone();

            (async move {
                processed.set_polled(value.id).await;

                tokio::time::sleep(std::time::Duration::from_millis(10 * value.duration as u64))
                    .await;

                processed.set_completed(value.id).await;

                value.id
            })
            .boxed()
        }
    }

    pub async fn debug(&self) {
        let timings = self.0.lock().await;

        let min_start = timings
            .iter()
            .map(|t| match t {
                (_, FutureTiming::Polled(start)) | (_, FutureTiming::Completed(start, _)) => start,
            })
            .min()
            .unwrap()
            .clone();

        dbg!(min_start);

        for (id, timing) in timings.iter() {
            match timing {
                FutureTiming::Polled(start) => {
                    println!(
                        "({}) polled - {:?}",
                        id,
                        start.duration_since(min_start).unwrap()
                    );
                }
                FutureTiming::Completed(start, end) => {
                    println!(
                        "({}) {:?} - {:?}",
                        id,
                        start.duration_since(min_start).unwrap(),
                        end.duration_since(min_start).unwrap()
                    );
                }
            }
        }
    }
}