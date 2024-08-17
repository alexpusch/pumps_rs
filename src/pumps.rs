use std::future::Future;

use futures::{
    channel::mpsc::{Receiver, Sender},
    stream::{FuturesOrdered, FuturesUnordered},
    SinkExt, Stream, StreamExt,
};

use crate::{
    execute_futures::{Concurrency, ExecuteFuturesPump},
    map::MapPump,
};

pub trait Pump<In, Out> {
    fn spawn(self, input_receiver: Receiver<In>) -> Receiver<Out>;
}

pub struct Pipeline<Out> {
    pub output_receiver: Receiver<Out>,
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

        Pipeline { output_receiver }
    }
}

impl<Out> Pipeline<Out> {
    pub fn new(output_receiver: Receiver<Out>) -> Self {
        Self { output_receiver }
    }

    pub fn from(source: impl IntoPipeline<Out>) -> Self {
        source.into_pipeline()
    }

    pub fn pump<P, POut>(self, pump: P) -> Pipeline<POut>
    where
        P: Pump<Out, POut>,
    {
        let pump_output_receiver = pump.spawn(self.output_receiver);

        Pipeline {
            output_receiver: pump_output_receiver,
        }
    }

    pub fn map<F, POut>(self, map_fn: F) -> Pipeline<POut>
    where
        F: Fn(Out) -> POut + Send + 'static,
        POut: Send + 'static,
        Out: Send + 'static,
    {
        self.pump(MapPump { map_fn })
    }

    pub fn execute_futures<Pout>(self, concurrency: Concurrency) -> Pipeline<Pout>
    where
        Out: Future<Output = Pout> + Send + 'static,
        Pout: Send + 'static,
    {
        self.pump(ExecuteFuturesPump { concurrency })
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    async fn async_job(x: i32) -> i32 {
        println!("Processing {}", x);
        x * 2
    }

    #[tokio::test]
    async fn test_pipeline() {
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);

        let pipeline = Pipeline::new(input_receiver)
            .map(async_job)
            .execute_futures(Concurrency::concurrent(2).backpressure(100));

        let mut output_receiver = pipeline.output_receiver;
        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(4));
        assert_eq!(output_receiver.next().await, Some(6));

        drop(input_sender);

        assert_eq!(output_receiver.next().await, None);
    }

    #[tokio::test]
    async fn test_into_pipeline() {
        let stream = stream::iter(vec![1, 2, 3]);

        let pipeline = Pipeline::from(stream)
            .map(async_job)
            .execute_futures(Concurrency::concurrent(2).backpressure(100));

        let mut output_receiver = pipeline.output_receiver;

        assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(4));
        assert_eq!(output_receiver.next().await, Some(6));

        assert_eq!(output_receiver.next().await, None);
    }
}
