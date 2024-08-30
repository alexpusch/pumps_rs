use std::future::Future;

use futures::{channel::mpsc::Receiver, FutureExt, SinkExt, StreamExt};

use crate::{
    concurency::{Concurrency, FuturesContainer},
    pumps::Pump,
};

pub struct FIlterMapPump<F> {
    pub(crate) map_fn: F,
    pub(crate) concurrency: Concurrency,
}

impl<In, Out, F, Fut> Pump<In, Out> for FIlterMapPump<F>
where
    F: Fn(In) -> Fut + Send + 'static,
    Fut: Future<Output = Option<Out>> + Send,
    In: Send + 'static,
    Out: Send + 'static,
{
    fn spawn(self, input_receiver: Receiver<In>) -> Receiver<Out> {
        let (mut output_sender, output_receiver) = futures::channel::mpsc::channel(
            self.concurrency.concurrency + self.concurrency.back_pressure,
        );

        let max_concurrency = self.concurrency.concurrency;

        tokio::spawn(async move {
            let mut in_progress = FuturesContainer::new(self.concurrency.preserve_order);

            let mut input_receiver = input_receiver.fuse();
            loop {
                if in_progress.len() < max_concurrency {
                    futures::select_biased! {
                        input = input_receiver.select_next_some() => {
                            let fut = (self.map_fn)(input);
                            in_progress.push_back(fut);
                        },
                        output = in_progress.select_next_some().fuse() => {
                            if let Some(output) = output {
                                output_sender.send(output).await.unwrap();
                            }
                        }
                    }
                } else {
                    let output = in_progress.select_next_some().await;
                    if let Some(output) = output {
                        output_sender.send(output).await.unwrap();
                    }
                }
            }
        });

        output_receiver
    }
}

#[cfg(test)]
mod tests {

    use crate::test_utils::{FutureTimings, TestValue};

    use super::*;

    #[tokio::test]
    async fn serial() {
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);

        let timings = FutureTimings::new();

        let pump = FIlterMapPump {
            concurrency: Concurrency::serial(),
            map_fn: timings.get_tracked_fn(|value| (value.id % 2 == 1).then_some(value.id)),
        };

        let output_receiver = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;

        input_sender.send(TestValue::new(1, 30)).await.unwrap();
        input_sender.send(TestValue::new(2, 20)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(1));
        // assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(3));

        assert!(timings.run_after(3, 2).await);
        assert!(timings.run_after(2, 1).await);

        drop(input_sender);

        assert_eq!(output_receiver.next().await, None);
    }

    #[tokio::test]
    async fn concurrency_2_unordered() {
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);

        let timings = FutureTimings::new();

        let pump = FIlterMapPump {
            concurrency: Concurrency::concurrent(2),
            map_fn: timings.get_tracked_fn(|value| Some(value.id)),
        };

        let output_receiver = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;

        // (2) finsihses first, (1) and (3) are executed concurrently
        input_sender.send(TestValue::new(1, 20)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(1));
        assert_eq!(output_receiver.next().await, Some(3));

        assert!(timings.run_in_parallel(1, 2).await);
        assert!(timings.run_in_parallel(1, 3).await);
        assert!(timings.run_after(3, 2).await);

        drop(input_sender);

        assert_eq!(output_receiver.next().await, None);
    }

    #[tokio::test]
    async fn concurrency_2_ordered() {
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);

        let timings = FutureTimings::new();

        let pump = FIlterMapPump {
            concurrency: Concurrency::concurrent(2).preserve_order(),
            map_fn: timings.get_tracked_fn(|value| Some(value.id)),
        };

        let output_receiver = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;

        // (2) finsihses first, but (2) and (3) are executed concurrently to keep order
        input_sender.send(TestValue::new(1, 20)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(1));
        assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(3));

        assert!(timings.run_in_parallel(1, 2).await);
        assert!(timings.run_after(3, 2).await);

        drop(input_sender);

        assert_eq!(output_receiver.next().await, None);
    }

    #[tokio::test]
    async fn concurrency_2_ordered_backpressure_3() {
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);

        let timings = FutureTimings::new();

        let pump = FIlterMapPump {
            concurrency: Concurrency::concurrent(2).backpressure(1),
            map_fn: timings.get_tracked_fn(|value| Some(value.id)),
        };

        let output_receiver = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;

        // (1), (2) finsihses first. (3) will run in parallel thanks to back pressure
        input_sender.send(TestValue::new(1, 20)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(1));
        assert_eq!(output_receiver.next().await, Some(3));

        timings.debug().await;

        assert!(timings.run_in_parallel(1, 2).await);
        assert!(timings.run_in_parallel(1, 3).await);
        assert!(timings.run_after(3, 2).await);
        // assert!(timings.run_after(3, 2).await);

        drop(input_sender);

        assert_eq!(output_receiver.next().await, None);
    }
}
