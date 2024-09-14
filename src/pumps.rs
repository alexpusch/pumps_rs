use std::future::Future;

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, Stream, StreamExt};
use tokio::{
    sync::mpsc::{self, Receiver},
    task::{JoinError, JoinHandle},
};

use crate::{concurrency::Concurrency, filter_map::FilterMapPump, map::MapPump, map_ok::MapOkPump};

pub trait Pump<In, Out> {
    fn spawn(self, input_receiver: Receiver<In>) -> (Receiver<Out>, JoinHandle<()>);
}

pub struct Pipeline<Out> {
    output_receiver: Receiver<Out>,
    handles: FuturesUnordered<JoinHandle<()>>,
}

impl<Out> From<Receiver<Out>> for Pipeline<Out> {
    fn from(receiver: Receiver<Out>) -> Self {
        Pipeline {
            output_receiver: receiver,
            handles: FuturesUnordered::new(),
        }
    }
}

impl<Out, PErr> Pipeline<Result<Out, PErr>> {
    pub fn map_ok<F, POut, PFut>(
        self,
        map_fn: F,
        concurrency: Concurrency,
    ) -> Pipeline<Result<POut, PErr>>
    where
        F: Fn(Out) -> PFut + Send + 'static,
        PFut: Future<Output = POut> + Send,
        POut: Send + 'static,
        PErr: Send + 'static,
        Out: Send + 'static,
    {
        self.pump(MapOkPump {
            map_fn,
            concurrency,
        })
    }
}

impl<Out> Pipeline<Out>
where
    Out: Send + 'static,
{
    pub fn from_stream(stream: impl Stream<Item = Out> + Send + 'static) -> Self {
        let (output_sender, output_receiver) = mpsc::channel(1);

        let h = tokio::spawn(async move {
            tokio::pin!(stream);
            while let Some(output) = stream.next().await {
                if let Err(_e) = output_sender.send(output).await {
                    break;
                }
            }
        });

        Pipeline {
            output_receiver,
            handles: [h].into_iter().collect(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Out> + Send + 'static,
        <I as IntoIterator>::IntoIter: std::marker::Send,
    {
        let (output_sender, output_receiver) = mpsc::channel(1);

        let h = tokio::spawn(async move {
            let iter = iter.into_iter();

            for output in iter {
                if let Err(_e) = output_sender.send(output).await {
                    break;
                };
            }
        });

        Pipeline {
            output_receiver,
            handles: [h].into_iter().collect(),
        }
    }

    pub fn pump<P, POut>(self, pump: P) -> Pipeline<POut>
    where
        P: Pump<Out, POut>,
    {
        let (pump_output_receiver, join_handle) = pump.spawn(self.output_receiver);
        let handles = self.handles;
        handles.push(join_handle);

        Pipeline {
            output_receiver: pump_output_receiver,
            handles,
        }
    }

    pub fn map<F, POut, PFut>(self, map_fn: F, concurrency: Concurrency) -> Pipeline<POut>
    where
        F: Fn(Out) -> PFut + Send + 'static,
        PFut: Future<Output = POut> + Send + 'static,
        POut: Send + 'static,
        Out: Send + 'static,
    {
        self.pump(MapPump {
            map_fn,
            concurrency,
        })
    }

    pub fn filter_map<F, POut, PFut>(self, map_fn: F, concurrency: Concurrency) -> Pipeline<POut>
    where
        F: Fn(Out) -> PFut + Send + 'static,
        PFut: Future<Output = Option<POut>> + Send + 'static,
        POut: Send + 'static,
        Out: Send + 'static,
    {
        self.pump(FilterMapPump {
            map_fn,
            concurrency,
        })
    }

    pub fn abort(self) {
        for handle in self.handles {
            handle.abort();
        }
    }

    pub fn build(mut self) -> (Receiver<Out>, BoxFuture<'static, Result<(), JoinError>>) {
        let join_result = async move {
            while let Some(res) = self.handles.next().await {
                match res {
                    Ok(_) => continue,
                    Err(e) => return Err(e),
                }
            }

            Ok(())
        };

        (self.output_receiver, join_result.boxed())
    }
}

#[cfg(test)]
mod tests {
    use futures::{stream, SinkExt};

    use super::*;

    async fn async_job(x: i32) -> i32 {
        x
    }

    async fn async_filter_map(x: i32) -> Option<i32> {
        if x % 2 == 0 {
            Some(x)
        } else {
            None
        }
    }

    #[tokio::test]
    async fn test_pipeline() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let pipeline = Pipeline::from(input_receiver)
            .map(
                async_job,
                Concurrency::concurrent_unordered(2).backpressure(100),
            )
            .filter_map(async_filter_map, Concurrency::serial());

        let (mut output_receiver, join_handle) = pipeline.build();
        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();
        input_sender.send(4).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(4));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, None);

        assert!(matches!(join_handle.await, Ok(())));
    }

    #[tokio::test]
    async fn panic_handling() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .map(
                async_job,
                Concurrency::concurrent_unordered(2).backpressure(100),
            )
            .map(
                |x| async move {
                    if x == 2 {
                        panic!("2 is not supported");
                    }

                    x
                },
                Concurrency::concurrent_unordered(2).backpressure(100),
            )
            .build();

        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, None);
        assert_eq!(output_receiver.recv().await, None);

        let res = join_handle.await;

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_from_stream() {
        let stream = stream::iter(vec![1, 2, 3]);

        let pipeline = Pipeline::from_stream(stream).map(async_job, Concurrency::serial());

        let mut output_receiver = pipeline.output_receiver;

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(3));

        assert_eq!(output_receiver.recv().await, None);
    }

    #[tokio::test]
    async fn test_from_futures_channel() {
        let (mut sender, receiver) = futures::channel::mpsc::channel(100);

        sender.send(1).await.unwrap();
        sender.send(2).await.unwrap();
        sender.send(3).await.unwrap();

        let pipeline = Pipeline::from_stream(receiver).map(async_job, Concurrency::serial());

        let mut output_receiver = pipeline.output_receiver;
        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(3));

        drop(sender);

        assert_eq!(output_receiver.recv().await, None);
    }

    #[tokio::test]
    async fn test_from_iter() {
        let iter = vec![1, 2, 3];

        let pipeline = Pipeline::from_iter(iter).map(async_job, Concurrency::serial());

        let mut output_receiver = pipeline.output_receiver;

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(3));

        assert_eq!(output_receiver.recv().await, None);
    }
}
