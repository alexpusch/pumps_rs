use std::future::Future;
use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::{
    concurency::{Concurrency, FuturesContainer},
    pumps::Pump,
};

pub struct FilterMapPump<F> {
    pub(crate) map_fn: F,
    pub(crate) concurrency: Concurrency,
}

impl<In, Out, F, Fut> Pump<In, Out> for FilterMapPump<F>
where
    F: Fn(In) -> Fut + Send + 'static,
    Fut: Future<Output = Option<Out>> + Send,
    In: Send + 'static,
    Out: Send + 'static,
{
    fn spawn(self, mut input_receiver: Receiver<In>) -> (Receiver<Out>, JoinHandle<()>) {
        let (output_sender, output_receiver) =
            mpsc::channel(self.concurrency.concurrency + self.concurrency.backpressure);

        let max_concurrency = self.concurrency.concurrency;

        let join_handle = tokio::spawn(async move {
            let mut in_progress = FuturesContainer::new(self.concurrency.preserve_order);

            loop {
                let in_progress_len = in_progress.len();

                tokio::select! {
                    biased;

                    Some(input) = input_receiver.recv(), if in_progress_len < max_concurrency => {
                        let fut = (self.map_fn)(input);
                        in_progress.push_back(fut);
                    },
                    Some(output) = in_progress.next(), if in_progress_len > 0 => {
                        if let Some(output) = output {
                            if let Err(_e) =  output_sender.send(output).await {
                                break;
                            }
                        }
                    },
                    else => break

                }
            }
        });

        (output_receiver, join_handle)
    }
}

#[cfg(test)]
mod tests {

    use crate::test_utils::{FutureTimings, TestValue};

    use super::*;

    #[tokio::test]
    async fn serial() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let timings = FutureTimings::new();

        let pump = FilterMapPump {
            concurrency: Concurrency::serial(),
            map_fn: timings.get_tracked_fn(|value| (value.id % 2 == 1).then_some(value.id)),
        };

        let (output_receiver, _) = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;

        input_sender.send(TestValue::new(1, 30)).await.unwrap();
        input_sender.send(TestValue::new(2, 20)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(3));

        assert!(timings.run_after(3, 2).await);
        assert!(timings.run_after(2, 1).await);

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);
    }

    #[tokio::test]
    async fn concurrency_2_unordered() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let timings = FutureTimings::new();

        let pump = FilterMapPump {
            concurrency: Concurrency::concurrent_unordered(2),
            map_fn: timings.get_tracked_fn(|value| Some(value.id)),
        };

        let (output_receiver, _) = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;

        // (2) finsihses first, (1) and (3) are executed concurrently
        input_sender.send(TestValue::new(1, 20)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(3));

        assert!(timings.run_in_parallel(1, 2).await);
        assert!(timings.run_in_parallel(1, 3).await);
        assert!(timings.run_after(3, 2).await);

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);
    }

    #[tokio::test]
    async fn concurrency_2_ordered() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let timings = FutureTimings::new();

        let pump = FilterMapPump {
            concurrency: Concurrency::concurrent_ordered(2),
            map_fn: timings.get_tracked_fn(|value| Some(value.id)),
        };

        let (output_receiver, _) = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;

        // (2) finsihses first, but (2) and (3) are executed concurrently to keep order
        input_sender.send(TestValue::new(1, 20)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(3));

        assert!(timings.run_in_parallel(1, 2).await);
        assert!(timings.run_after(3, 2).await);

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);
    }

    #[tokio::test]
    async fn concurrency_2_ordered_backpressure_3() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let timings = FutureTimings::new();

        let pump = FilterMapPump {
            concurrency: Concurrency::concurrent_unordered(2).backpressure(1),
            map_fn: timings.get_tracked_fn(|value| Some(value.id)),
        };

        let (output_receiver, _) = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;

        // (1), (2) finsihses first. (3) will run in parallel thanks to back pressure
        input_sender.send(TestValue::new(1, 20)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(3));

        timings.debug().await;

        assert!(timings.run_in_parallel(1, 2).await);
        assert!(timings.run_in_parallel(1, 3).await);
        assert!(timings.run_after(3, 2).await);
        // assert!(timings.run_after(3, 2).await);

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);
    }
}
