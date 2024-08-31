use std::future::Future;

use futures::{
    channel::mpsc::Receiver, future::BoxFuture, stream::FuturesUnordered, FutureExt, SinkExt,
    Stream, StreamExt,
};
use tokio::task::{JoinError, JoinHandle};

use crate::{concurency::Concurrency, filter_map::FilterMapPump, map::MapPump};

pub trait Pump<In, Out> {
    fn spawn(self, input_receiver: Receiver<In>) -> (Receiver<Out>, JoinHandle<()>);
}

pub struct Pipeline<Out> {
    output_receiver: Receiver<Out>,
    handles: FuturesUnordered<JoinHandle<()>>,
}

pub trait IntoPipeline<Out> {
    fn into_pipeline(self) -> Pipeline<Out>;
}

impl<Out, T> IntoPipeline<Out> for T
where
    T: Stream<Item = Out> + Send + 'static,
    Out: Send + 'static,
{
    fn into_pipeline(self) -> Pipeline<Out> {
        let (mut output_sender, output_receiver) = futures::channel::mpsc::channel(1);
        tokio::spawn(async move {
            let stream = self.fuse();
            tokio::pin!(stream);
            while let Some(output) = stream.next().await {
                output_sender.send(output).await.unwrap();
            }
        });

        Pipeline {
            output_receiver,
            handles: FuturesUnordered::new(),
        }
    }
}

impl<Out> Pipeline<Out>
where
    Out: Send + 'static,
{
    pub fn new(output_receiver: Receiver<Out>) -> Self {
        Self {
            output_receiver,
            handles: FuturesUnordered::new(),
        }
    }

    pub fn from(source: impl IntoPipeline<Out>) -> Self {
        source.into_pipeline()
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
    use futures::stream;

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
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);

        let pipeline = Pipeline::new(input_receiver)
            .map(async_job, Concurrency::concurrent(2).backpressure(100))
            .filter_map(async_filter_map, Concurrency::serial());

        let (mut output_receiver, join_handle) = pipeline.build();
        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();
        input_sender.send(4).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(4));

        drop(input_sender);
        assert_eq!(output_receiver.next().await, None);

        assert!(matches!(join_handle.await, Ok(())));
    }

    #[tokio::test]
    async fn panic_handling() {
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::new(input_receiver)
            .map(async_job, Concurrency::concurrent(2).backpressure(100))
            .map(
                |x| async move {
                    if x == 2 {
                        panic!("2 is not supported");
                    }

                    x
                },
                Concurrency::concurrent(2).backpressure(100),
            )
            .build();

        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(1));
        assert_eq!(output_receiver.next().await, None);
        assert_eq!(output_receiver.next().await, None);

        let res = join_handle.await;

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_into_pipeline() {
        let stream = stream::iter(vec![1, 2, 3]);

        let pipeline =
            Pipeline::from(stream).map(async_job, Concurrency::concurrent(2).backpressure(100));

        let mut output_receiver = pipeline.output_receiver;

        assert_eq!(output_receiver.next().await, Some(1));
        assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(3));

        assert_eq!(output_receiver.next().await, None);
    }
}
