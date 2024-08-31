use std::future::Future;

use futures::{channel::mpsc::Receiver, SinkExt, StreamExt};
use tokio::task::JoinHandle;

use crate::{
    concurency::{Concurrency, FuturesContainer},
    pumps::Pump,
};

pub struct MapPump<F> {
    pub(crate) map_fn: F,
    pub(crate) concurrency: Concurrency,
}

impl<In, Out, F, Fut> Pump<In, Out> for MapPump<F>
where
    F: Fn(In) -> Fut + Send + 'static,
    Fut: Future<Output = Out> + Send,
    In: Send + 'static,
    Out: Send + 'static,
{
    fn spawn(self, mut input_receiver: Receiver<In>) -> (Receiver<Out>, JoinHandle<()>) {
        let (mut output_sender, output_receiver) = futures::channel::mpsc::channel(
            self.concurrency.concurrency + self.concurrency.back_pressure,
        );

        let max_concurrency = self.concurrency.concurrency;

        let join_handle = tokio::spawn(async move {
            let mut in_progress = FuturesContainer::new(self.concurrency.preserve_order);

            loop {
                let in_progress_len = in_progress.len();

                tokio::select! {
                    biased;

                    Some(input) = input_receiver.next(), if in_progress_len < max_concurrency => {
                        let fut = (self.map_fn)(input);
                        in_progress.push_back(fut);
                    },
                    Some(output) = in_progress.next(), if in_progress_len > 0 => {
                        if let Err(_e) = output_sender.send(output).await {
                            break;
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
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);

        let timings = FutureTimings::new();

        let pump = MapPump {
            concurrency: Concurrency::serial(),
            map_fn: timings.get_tracked_fn(|value| value.id),
        };

        let (output_receiver, _) = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;

        input_sender.send(TestValue::new(1, 30)).await.unwrap();
        input_sender.send(TestValue::new(2, 20)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(1));
        assert_eq!(output_receiver.next().await, Some(2));
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

        let pump = MapPump {
            concurrency: Concurrency::concurrent(2),
            map_fn: timings.get_tracked_fn(|value| value.id),
        };

        let (output_receiver, _) = pump.spawn(input_receiver);

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

        let pump = MapPump {
            concurrency: Concurrency::concurrent(2).preserve_order(),
            map_fn: timings.get_tracked_fn(|value| value.id),
        };

        let (output_receiver, _) = pump.spawn(input_receiver);

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

        let pump = MapPump {
            concurrency: Concurrency::concurrent(2).backpressure(1),
            map_fn: timings.get_tracked_fn(|value| value.id),
        };

        let (output_receiver, _) = pump.spawn(input_receiver);

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
