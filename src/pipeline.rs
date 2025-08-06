use std::future::Future;

use futures::{
    future::{self, BoxFuture},
    stream::FuturesUnordered,
    FutureExt, Stream, StreamExt,
};
use tokio::{
    sync::mpsc::{self, Receiver},
    task::{JoinError, JoinHandle},
};

use crate::{
    concurrency::Concurrency,
    pumps::{
        catch::CatchPump,
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
    #[allow(clippy::should_implement_trait)] // Clippy suggests implementing `FromIterator` or choosing a less ambiguous method name
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
    /// This method can be used to create custom `Pump` operations
    /// ```
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

    /// Apply the provided async map function on each item recieved from pipeline. The Future returned
    /// from the map function is executed concurrently according to the [Concurrency] configuration.
    /// The results of the executed futures is sent as output.
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

    /// Apply the provided async filter map function on each item recieved from the pipeline. The Future returned
    /// from the filter map function is executed concurrently according to the [Concurrency] configuration.
    /// If the invocation results in None the triggering item will be removed from the pipeline, otherwise the result
    /// will be sent as output.
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

    /// Transform each item in the pipeline into a tuple of the item and its index according to the arrival order.
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

    /// Batch items from the input pipeline into a vector of at most `n` items. While the buffer is not full,
    /// no batches will be emitted. If the pipeline input is finite, the last batch emitted will be partial.
    ///
    /// # Example
    /// ```rust
    /// use pumps::Pipeline;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (mut output, h) = Pipeline::from_iter(vec![1, 2, 3, 4, 5])
    ///   .batch(2)
    ///   .build();
    ///
    /// assert_eq!(output.recv().await, Some(vec![1, 2]));
    /// assert_eq!(output.recv().await, Some(vec![3, 4]));
    /// assert_eq!(output.recv().await, Some(vec![5]));
    /// assert_eq!(output.recv().await, None);
    /// # });
    pub fn batch(self, n: usize) -> Pipeline<Vec<Out>> {
        self.pump(crate::pumps::batch::BatchPump { n })
    }

    /// Batch items from the input pipeline while the provided function returns `Some`. Similarly to iterator 'fold' method,
    /// the function takes a state and the current item and returns a new state. This allows the user to control when to emit a batch
    /// based on the accumulated state. On batch emission, the state is reset.
    ///
    /// If the pipeline input is finite, the last item emitted will be partial.
    ///
    /// # Example
    /// ```rust
    /// use pumps::Pipeline;
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (mut output, h) = Pipeline::from_iter(vec![1, 2, 3, 4, 5])
    /// // batch until accumulated sum of 10
    /// .batch_while(0, |state, x| {
    ///     let sum = state + x;
    ///
    ///     (sum < 10).then_some(sum)
    /// })
    ///   .build();
    ///
    /// assert_eq!(output.recv().await, Some(vec![1, 2, 3, 4]));
    /// assert_eq!(output.recv().await, Some(vec![5]));
    /// assert_eq!(output.recv().await, None);
    /// # });
    pub fn batch_while<F, State>(self, state_init: State, mut while_fn: F) -> Pipeline<Vec<Out>>
    where
        F: FnMut(State, &Out) -> Option<State> + Send + 'static,
        State: Send + Clone + 'static,
    {
        self.pump(
            crate::pumps::batch_while_with_expiry::BatchWhileWithExpiryPump {
                state_init,
                while_fn: move |state: State, x: &Out| {
                    let new_state = while_fn(state.clone(), x);
                    // no exipry. The future is never resolved
                    new_state.map(|new_state| (new_state, future::pending()))
                },
            },
        )
    }

    /// Batch items from the input pipeline while the provided function returns `Some`. Similar to the iterator `fold` method,
    /// the function takes a state and an item, and returns a new state. This allows the user to control when to emit a batch
    /// based on the accumulated state.
    ///
    /// This variant of batch allows the user to return a future in addition to the new batch state. If this future resolves
    /// before the batch is emitted, the batch will be emitted, and the state will be reset. Only the most
    /// recent future is considered. For example, this feature can be used to implement a timeout for the batch.
    ///
    /// If the pipeline input is finite, the last batch emitted will be partial.
    ///
    /// # Example
    /// ```rust
    /// use pumps::Pipeline;
    /// use std::time::Duration;
    /// use tokio::{time::sleep, sync::mpsc};
    ///
    /// # async fn send(input_sender: &mpsc::Sender<i32>, data: Vec<i32>) {
    /// #   for x in data {
    /// #       input_sender.send(x).await.unwrap();
    /// #   }
    /// # }
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (input_sender, input_receiver) = mpsc::channel(100);
    ///
    /// let (mut output, h) = Pipeline::from(input_receiver)
    /// .batch_while_with_expiry(0, |state, x| {
    ///     let sum = state + x;
    ///
    ///     // batch until accumulated sum of 10, or 100ms passed without activity
    ///     (sum < 10).then_some((sum, sleep(Duration::from_millis(100))))
    /// })
    ///   .build();
    ///
    /// send(&input_sender, vec![1,2]).await;
    /// sleep(Duration::from_millis(200)).await;
    /// assert_eq!(output.recv().await, Some(vec![1, 2]));
    ///
    /// send(&input_sender, vec![3, 3, 4]).await;
    /// assert_eq!(output.recv().await, Some(vec![3, 3, 4]));
    ///
    /// drop(input_sender);
    /// assert_eq!(output.recv().await, None);
    /// # });
    pub fn batch_while_with_expiry<F, Fut, State>(
        self,
        state_init: State,
        while_fn: F,
    ) -> Pipeline<Vec<Out>>
    where
        F: FnMut(State, &Out) -> Option<(State, Fut)> + Send + 'static,
        Fut: Future<Output = ()> + Send,
        State: Send + Clone + 'static,
    {
        self.pump(
            crate::pumps::batch_while_with_expiry::BatchWhileWithExpiryPump {
                state_init,
                while_fn,
            },
        )
    }

    /// Skip the first `n` items in the pipeline.
    pub fn skip(self, n: usize) -> Pipeline<Out> {
        self.pump(crate::pumps::skip::SkipPump { n })
    }

    /// Take the first `n` items in the pipeline, removing the rest.
    pub fn take(self, n: usize) -> Pipeline<Out> {
        self.pump(crate::pumps::take::TakePump { n })
    }

    /// Apply backpressure by bufferring up to `n` items between the previous pump to the next one.
    /// When a downstream operation slows down, backpressure will allow some items to accumulate, tempreraly preventing upstream blocking.
    ///     
    /// # Example
    /// ```rust
    /// use pumps::{Pipeline, Concurrency};
    ///
    /// # async fn mostly_fast_fn(x: i32) -> i32 {
    /// #   x
    /// # }
    /// #
    /// # async fn sometimes_slow_fn(x: i32) -> i32 {
    /// #    x
    /// # }
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (mut output, h) = Pipeline::from_iter(vec![1, 2, 3])
    ///     .map(mostly_fast_fn, Concurrency::serial())
    ///     // This will allow up to 32 `mostly_fast_fn` results to accumulate while `sometimes_slow_fn` slows down
    ///     // Without this `mostly_fast_fn` will be idle waiting for `sometimes_slow_fn` to catch up.
    ///     .backpressure(32)
    ///     .map(sometimes_slow_fn, Concurrency::serial())
    ///     .build();
    /// # });
    pub fn backpressure(self, n: usize) -> Pipeline<Out> {
        self.pump(crate::pumps::backpressure::Backpressure { n })
    }

    /// Apply backpressure by bufferring up to `n` items between the previous pump to the next one, dropping the oldest items when the buffer is full.
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

impl<Err, Out> Pipeline<Result<Out, Err>>
where
    Err: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    /// Catches errors from a pipeline of [`Result`]s, sending them to a separate channel.
    ///
    /// The pipeline will continue processing subsequent items, even after encountering an error.
    ///
    /// # Notes
    /// - Dropping `error_rx` will not stop the pipeline. If we cannot send an error, we ignore it and continue.
    /// - If the output channel of this pipeline is dropped, we send all errors in the input pipeline to `channel`.
    ///
    /// ```rust
    /// use pumps::Pipeline;
    /// use tokio::sync::mpsc;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let (error_tx, mut error_rx) = mpsc::channel(10);
    ///
    /// let (mut output, h) = Pipeline::from_iter(vec![
    ///     Ok(1),
    ///     Err("first error"),
    ///     Ok(2),
    ///     Err("second error"),
    ///     Ok(3)
    /// ])
    ///     .catch(error_tx)
    ///     .build();
    ///
    /// assert_eq!(output.recv().await, Some(1));
    /// assert_eq!(output.recv().await, Some(2));
    /// assert_eq!(output.recv().await, Some(3));
    /// assert_eq!(output.recv().await, None);
    ///
    /// assert_eq!(error_rx.recv().await, Some("first error"));
    /// assert_eq!(error_rx.recv().await, Some("second error"));
    /// assert_eq!(error_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Disconnected);
    /// # });
    /// ```
    pub fn catch(self, channel: tokio::sync::mpsc::Sender<Err>) -> Pipeline<Out> {
        self.pump(CatchPump {
            channel,
            abort_on_error: false,
        })
    }

    /// Catches errors from a pipeline of [`Result`]s, sending them to a separate channel.
    ///
    /// This behaves exactly like [`Self::catch`], but processes no items after
    /// encountering an [`Err`]. This pump will send at most one error to `channel`.
    pub fn catch_abort(self, channel: tokio::sync::mpsc::Sender<Err>) -> Pipeline<Out> {
        self.pump(CatchPump {
            channel,
            abort_on_error: true,
        })
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

    #[tokio::test]
    async fn test_custom_pump() {
        pub struct CustomPump;

        impl<In> Pump<In, In> for CustomPump
        where
            In: Send + Sync + Clone + 'static,
        {
            fn spawn(self, mut input_receiver: Receiver<In>) -> (Receiver<In>, JoinHandle<()>) {
                let (output_sender, output_receiver) = mpsc::channel(1);

                let h = tokio::spawn(async move {
                    while let Some(input) = input_receiver.recv().await {
                        if let Err(_e) = output_sender.send(input.clone()).await {
                            break;
                        }

                        if let Err(_e) = output_sender.send(input).await {
                            break;
                        }
                    }
                });

                (output_receiver, h)
            }
        }

        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) =
            Pipeline::from(input_receiver).pump(CustomPump).build();

        input_sender.send(1).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(1));

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);

        join_handle.await.unwrap();
    }
}
