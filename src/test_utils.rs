use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures::{future::BoxFuture, FutureExt};
use tokio::sync::{mpsc, Mutex};

pub struct TestValue {
    pub id: i32,
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
    #[allow(clippy::new_without_default)]
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

    pub async fn is_completed(&self, id: i32) -> bool {
        let timings = self.0.lock().await;

        matches!(timings.get(&id), Some(FutureTiming::Completed(_, _)))
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

    pub fn get_tracked_fn<F, FOut>(
        &self,
        inner_fn: F,
    ) -> impl Fn(TestValue) -> BoxFuture<'static, FOut>
    where
        F: Fn(&TestValue) -> FOut + Send + Sync + 'static,
        FOut: Send + 'static,
    {
        let this = self.clone();

        move |value| {
            let timings = this.clone();
            let output = (inner_fn)(&value);

            (async move {
                timings.set_polled(value.id).await;

                // TODO - do not rely on time for testing
                tokio::time::sleep(std::time::Duration::from_millis(10 * value.duration)).await;

                timings.set_completed(value.id).await;

                output
            })
            .boxed()
        }
    }

    #[allow(unused)]
    pub async fn debug(&self) {
        let timings = self.0.lock().await;

        let min_start = *timings
            .iter()
            .map(|t| match t {
                (_, FutureTiming::Polled(start)) | (_, FutureTiming::Completed(start, _)) => start,
            })
            .min()
            .unwrap();

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

pub async fn wait_for_capacity(sender: &mpsc::Sender<i32>) -> usize {
    tokio::time::sleep(Duration::from_millis(20)).await;
    sender.capacity()
}

pub async fn wait_for_len<T>(receiver: &mpsc::Receiver<T>) -> usize {
    tokio::time::sleep(Duration::from_millis(20)).await;
    receiver.len()
}
