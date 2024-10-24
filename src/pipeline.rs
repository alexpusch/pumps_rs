use std::future::Future;

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, Stream, StreamExt};
use tokio::{
    sync::mpsc::{self, Receiver},
    task::{JoinError, JoinHandle},
};

use crate::{
    concurrency::Concurrency,
    pumps::{
        filter_map::FilterMapPump,
        flatten::{FlattenConcurrency, FlattenPump},
        map::MapPump,
        map_err::MapErrPump,
        map_ok::MapOkPump,
    },
    Pump,
};

/// A `Pipeline` is the builder API for a series of `Pump` operations.
///
/// A Pipeline is constructed out of an `Iterator`, a `Stream` or a `Receiver`. After constructing the pipeline, additional pumps can be attached to it, filtering, transforming or perform other operations on the data.
/// After defining the needed operations, the `.build()` method is used to obtain the output `Receiver` and a `JoinHandle` to the internally spawned tasks.
/// To use the pipeline result, use the output Receiver as you wish.
///
/// ## Concurrency
/// Most pipeline operations are preformed concurrently. The concurrency characteristics of each operation can be controlled by the [`Concurrency`] module.
///
/// # Example
/// ```rust
/// use pumps::{Pipeline, Concurrency};
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let (mut output, h) = Pipeline::from_iter(vec![1, 2, 3, 4, 5])
///     .filter_map(|x| async move { (x %2 == 0).then_some(x)}, Concurrency::concurrent_ordered(8))
///     .map(|x| async move { x * 2 }, Concurrency::concurrent_ordered(8))
///     .backpressure(100)
///     .map(|x| async move { x + 1 }, Concurrency::serial())
///     .build();
///
/// assert_eq!(output.recv().await, Some(5));
/// assert_eq!(output.recv().await, Some(9));
/// assert_eq!(output.recv().await, None);
/// # });
/// ```
/// ## Panic handling
/// Since Pumps are spawned as tasks, they can panic. On panic, all inner tasks and channels will close.
/// The `.build()` method returns a `JoinHandle` that can be awaited to check if the pipeline has finished successfully.
///
/// ```rust
/// use pumps::{Pipeline, Concurrency};
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let (mut output, h) = Pipeline::from_iter(vec![1, 2, 3])
///     .map(|x| async move { panic!("oh no"); x }, Concurrency::concurrent_ordered(8))
///     .build();
///
/// while let Some(output) = output.recv().await {
///    println!("received output {}", output);
/// }
///
/// let res =  h.await;
///
/// assert!(res.is_err());
/// # });
/// ```
pub struct Pipeline<Out> {
    pub(crate) output_receiver: Receiver<Out>,
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

impl<Out> Pipeline<Out>
where
    Out: Send + 'static,
{
    /// Construct a [Pipeline] from a [futures_util::stream::Stream](https://docs.rs/futures/latest/futures/stream/index.html)
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

    /// Construct a [`Pipeline`] from an [`IntoIterator`]
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

    /// attach an additional `Pump` to the pipeline
    /// This method can be used to create custom `Pump` oprations
    pub fn pump<P, T>(self, pump: P) -> Pipeline<T>
    where
        P: Pump<Out, T>,
    {
        let (pump_output_receiver, join_handle) = pump.spawn(self.output_receiver);
        let handles = self.handles;
        handles.push(join_handle);

        Pipeline {
            output_receiver: pump_output_receiver,
            handles,
        }
    }

    /// Attach a map pump to the pipeline. Map will apply the provided function to each item.
    ///
    /// # Example
    /// ```rust
    /// use pumps::{Pipeline, Concurrency};
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (mut output, h) = Pipeline::from_iter(vec![1, 2, 3])
    ///     .map(|x| async move { x * 2 }, Concurrency::concurrent_ordered(8))
    ///     .build();
    ///
    /// assert_eq!(output.recv().await, Some(2));
    /// assert_eq!(output.recv().await, Some(4));
    /// assert_eq!(output.recv().await, Some(6));
    /// assert_eq!(output.recv().await, None);
    /// # });
    /// ```
    pub fn map<F, Fut, T>(self, map_fn: F, concurrency: Concurrency) -> Pipeline<T>
    where
        F: Fn(Out) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        Out: Send + 'static,
    {
        self.pump(MapPump {
            map_fn,
            concurrency,
        })
    }

    /// Attach a filter_map pump to the pipeline. Filter map will apply the provided function to each item, removing items that avaluate to None from the pipeline.
    ///
    /// # Example
    /// ```rust
    /// use pumps::{Pipeline, Concurrency};
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (mut output, h) = Pipeline::from_iter(vec![1, 2, 3])
    ///     .filter_map(|x| async move { (x % 2 == 0).then_some(x) }, Concurrency::concurrent_ordered(8))
    ///     .build();
    ///
    /// assert_eq!(output.recv().await, Some(2));
    /// assert_eq!(output.recv().await, None);
    /// # });
    /// ```
    pub fn filter_map<F, Fut, T>(self, map_fn: F, concurrency: Concurrency) -> Pipeline<T>
    where
        F: FnMut(Out) -> Fut + Send + 'static,
        Fut: Future<Output = Option<T>> + Send + 'static,
        T: Send + 'static,
        Out: Send + 'static,
    {
        self.pump(FilterMapPump {
            map_fn,
            concurrency,
        })
    }

    /// Attach an enumerate pump to the pipeline. Enumerate will add an index to each item in the pipeline.
    /// The index starts at 0.
    ///
    /// # Example
    /// ```rust
    /// use pumps::{Pipeline, Concurrency};
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (mut output, h) = Pipeline::from_iter(vec![1, 2])
    ///    .enumerate()
    ///   .build();
    ///
    /// assert_eq!(output.recv().await, Some((0, 1)));
    /// assert_eq!(output.recv().await, Some((1, 2)));
    /// assert_eq!(output.recv().await, None);
    ///
    /// # });
    pub fn enumerate(self) -> Pipeline<(usize, Out)> {
        self.pump(crate::pumps::enumerate::EnumeratePump)
    }

    /// Attach a skip pump to the pipeline. Skip will skip the first `n` items in the pipeline.
    pub fn skip(self, n: usize) -> Pipeline<Out> {
        self.pump(crate::pumps::skip::SkipPump { n })
    }

    /// Attach a take pump to the pipeline. Take will take the first `n` items in the pipeline.
    pub fn take(self, n: usize) -> Pipeline<Out> {
        self.pump(crate::pumps::take::TakePump { n })
    }

    /// Attach backpressure to the pipeline. Backpressure will buffer up to `n` items between two pumps in the pipeline.
    /// When a downstream operation slows down, backpressure will allow some items to accumulate, tempreraly preventing upstream blocking.
    pub fn backpressure(self, n: usize) -> Pipeline<Out> {
        self.pump(crate::pumps::backpressure::Backpressure { n })
    }

    /// Attach backpressure with relief valve to the pipeline. Backpressure will buffer up to `n` items between two pumps in the pipeline.
    /// When `n` items are buffered, the relief valve will start dropping the oldest items, buffering only the most recent ones.
    /// When a downstream operation slows down, backpressure with relief valve will allow recent items to accumulate, preventing upstream blocking
    /// in the costs of dropped data.
    pub fn backpressure_with_relief_valve(self, n: usize) -> Pipeline<Out> {
        self.pump(crate::pumps::backpressure_with_relief_valve::BackpressureWithReliefValve { n })
    }

    /// Returns the output receiver and a join handle - a future that resolves when all inner tasks have finished.
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

    /// aborts all inner tasks
    pub fn abort(self) {
        for handle in self.handles {
            handle.abort();
        }
    }
}

impl<Out> Pipeline<Pipeline<Out>>
where
    Out: Send + Sync + 'static,
{
    /// Given a pipeline of pipelines, flatten it into a single pipeline.
    /// Ordering is controlled by the [FlattenConcurrency] parameter.
    ///
    /// # Example
    /// ```rust
    /// use pumps::{Pipeline, FlattenConcurrency};
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (mut output, h) = Pipeline::from_iter(vec![Pipeline::from_iter(vec![1, 2]),Pipeline::from_iter(vec![3, 4])])
    ///     .flatten(FlattenConcurrency::ordered())
    ///     .build();
    ///
    /// assert_eq!(output.recv().await, Some(1));
    /// assert_eq!(output.recv().await, Some(2));
    /// assert_eq!(output.recv().await, Some(3));
    /// assert_eq!(output.recv().await, Some(4));
    /// assert_eq!(output.recv().await, None);
    /// # });
    /// ```
    pub fn flatten(self, concurrency: FlattenConcurrency) -> Pipeline<Out> {
        self.pump(FlattenPump { concurrency })
    }
}

impl<OutOk, OutErr> Pipeline<Result<OutOk, OutErr>> {
    /// applies a function to the success value for each item in a [Pipeline] of `Results<T, E>`
    ///
    /// # Example
    /// ```rust
    /// use pumps::{Pipeline, Concurrency};
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (mut output, h) = Pipeline::from_iter(vec![Ok(1), Err(()), Ok(2)])
    ///     .map_ok(|x| async move { x * 2 }, Concurrency::concurrent_ordered(8))
    ///     .build();
    ///
    /// assert_eq!(output.recv().await, Some(Ok(2)));
    /// assert_eq!(output.recv().await, Some(Err(())));
    /// assert_eq!(output.recv().await, Some(Ok(4)));
    /// assert_eq!(output.recv().await, None);
    /// # });
    /// ```
    pub fn map_ok<F, Fut, T>(
        self,
        map_fn: F,
        concurrency: Concurrency,
    ) -> Pipeline<Result<T, OutErr>>
    where
        F: Fn(OutOk) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send,
        T: Send + 'static,
        OutErr: Send + 'static,
        OutOk: Send + 'static,
    {
        self.pump(MapOkPump {
            map_fn,
            concurrency,
        })
    }

    /// applies a function to the error value for each item in a [Pipeline] of `Results<T, E>`
    ///
    /// # Example
    /// ```rust
    /// use pumps::{Pipeline, Concurrency};
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (mut output, h) = Pipeline::from_iter(vec![Ok(1), Err("oh no"), Ok(2)])
    ///     .map_err(|x| async move { format!("{x}!") }, Concurrency::concurrent_ordered(8))
    ///     .build();
    ///
    /// assert_eq!(output.recv().await, Some(Ok(1)));
    /// assert_eq!(output.recv().await, Some(Err("oh no!".to_string())));
    /// assert_eq!(output.recv().await, Some(Ok(2)));
    /// assert_eq!(output.recv().await, None);
    /// # });
    /// ```
    pub fn map_err<F, Fut, T>(
        self,
        map_fn: F,
        concurrency: Concurrency,
    ) -> Pipeline<Result<OutOk, T>>
    where
        F: Fn(OutErr) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send,
        T: Send + 'static,
        OutErr: Send + 'static,
        OutOk: Send + 'static,
    {
        self.pump(MapErrPump {
            map_fn,
            concurrency,
        })
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
            .map(async_job, Concurrency::concurrent_unordered(2))
            .backpressure(100)
            .map(async_job, Concurrency::concurrent_unordered(2))
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
            .map(async_job, Concurrency::concurrent_unordered(2))
            .backpressure(100)
            .map(
                |x| async move {
                    if x == 2 {
                        panic!("2 is not supported");
                    }

                    x
                },
                Concurrency::concurrent_unordered(2),
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
