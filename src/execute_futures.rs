use std::future::Future;

use futures::{channel::mpsc::Receiver, stream::FuturesOrdered, SinkExt, StreamExt};

use crate::pumps::Pump;

pub struct Concurrency {
    concurrency: usize,
    back_pressure: usize,
}

impl Concurrency {
    pub fn concurrent(concurrency: usize) -> Self {
        Self {
            concurrency,
            back_pressure: 64,
        }
    }

    pub fn serial() -> Self {
        Self {
            concurrency: 1,
            back_pressure: 64,
        }
    }

    pub fn backpressure(self, back_pressure: usize) -> Self {
        Self {
            concurrency: self.concurrency,
            back_pressure: back_pressure,
        }
    }
}

pub struct ExecuteFuturesPump {
    pub(crate) concurrency: Concurrency,
}

impl<In, Out> Pump<In, Out> for ExecuteFuturesPump
where
    In: Future<Output = Out> + Send + 'static,
    Out: Send + 'static,
{
    fn spawn(self, input_receiver: Receiver<In>) -> Receiver<Out> {
        let (mut output_sender, output_receiver) =
            futures::channel::mpsc::channel(self.concurrency.back_pressure);

        let max_concurrency = self.concurrency.concurrency;

        tokio::spawn(async move {
            let mut in_progress = FuturesOrdered::new();

            let mut input_receiver = input_receiver.fuse();
            loop {
                if in_progress.len() < max_concurrency {
                    futures::select_biased! {
                        input = input_receiver.select_next_some() => {
                            in_progress.push_back(input);
                        },
                        output = in_progress.select_next_some() => {
                            output_sender.send(output).await.unwrap();

                        }
                    }
                } else {
                    let output = in_progress.select_next_some().await;
                    output_sender.send(output).await.unwrap();
                }
            }
        });

        output_receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn async_job(x: i32) -> i32 {
        println!("Processing {}", x);
        x * 2
    }

    #[tokio::test]
    async fn test_concurrent_pump() {
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);
        let pump = ExecuteFuturesPump {
            concurrency: Concurrency::concurrent(2).backpressure(100),
        };

        let output_receiver = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;
        input_sender.send(async_job(1)).await.unwrap();
        input_sender.send(async_job(2)).await.unwrap();
        input_sender.send(async_job(3)).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(4));
        assert_eq!(output_receiver.next().await, Some(6));

        drop(input_sender);

        assert_eq!(output_receiver.next().await, None);
    }
}
