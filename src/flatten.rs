use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::pumps::{Pipeline, Pump};

/// Defines concurrency characteristics of a Flatten operation
/// Unline [`Concurrency`], this struct does not have a `concurrency` field. A `concurrency` value is not relevant
/// for a Flatten operation, as it is the upstream operations that are executed concurrently
pub struct FlattenConcurrency {
    /// How many future results can be stored in memory before a consumer receives them from the output channel.
    /// In other words, this is the size of the output channel.
    /// When the output channel is full, the operation will stop processing additioanl data
    /// Defaults to 1
    pub backpressure: usize,
    pub preserve_order: bool,
}

impl FlattenConcurrency {
    /// Defines a flatten operation that preserves the order of the input streams
    pub fn ordered() -> Self {
        Self {
            backpressure: 1,
            preserve_order: true,
        }
    }

    /// Defines a flatten operation that does not preserve the order of the input streams
    pub fn unordered() -> Self {
        Self {
            backpressure: 1,
            preserve_order: false,
        }
    }

    /// How many futures can be stored in memory before a consumer takes them from the output channel
    pub fn backpressure(self, backpressure: usize) -> Self {
        Self {
            backpressure,
            ..self
        }
    }
}

pub struct FlattenPump {
    pub(crate) concurrency: FlattenConcurrency,
}

impl<In> Pump<Pipeline<In>, In> for FlattenPump
where
    In: Send + Sync + 'static,
{
    fn spawn(self, mut input_receiver: Receiver<Pipeline<In>>) -> (Receiver<In>, JoinHandle<()>) {
        let (output_sender, output_receiver) = mpsc::channel(self.concurrency.backpressure);

        let h = tokio::spawn(async move {
            while let Some(mut pipeline) = input_receiver.recv().await {
                let output_sender = output_sender.clone();

                let inner_h = tokio::spawn(async move {
                    while let Some(output) = pipeline.output_receiver.recv().await {
                        if let Err(_e) = output_sender.send(output).await {
                            break;
                        }
                    }
                });

                // if we want to preserve order, we need to wait for the inner pipeline to finish
                if self.concurrency.preserve_order {
                    if let Err(_e) = inner_h.await {
                        break;
                    }
                }
            }
        });

        (output_receiver, h)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;

    #[tokio::test]
    async fn flatten_ordered() {
        let (input_sender, input_receiver) = mpsc::channel(100);
        let (mut output_receiver, _join_handle) = Pipeline::from(input_receiver)
            .flatten(FlattenConcurrency::ordered())
            .build();

        input_sender
            .send(Pipeline::from_iter(vec![1, 2, 3]))
            .await
            .unwrap();
        input_sender
            .send(Pipeline::from_iter(vec![3, 4, 5]))
            .await
            .unwrap();

        drop(input_sender);

        let mut results = vec![];

        while let Some(output) = output_receiver.recv().await {
            results.push(output);
        }

        // order is preserved
        assert_eq!(results, vec![1, 2, 3, 3, 4, 5]);
    }

    #[tokio::test]
    async fn flatten_unordered() {
        let (input_sender, input_receiver) = mpsc::channel(100);
        let (mut output_receiver, _join_handle) = Pipeline::from(input_receiver)
            .flatten(FlattenConcurrency::unordered())
            .build();

        input_sender
            .send(Pipeline::from_iter(vec![1, 2, 3]))
            .await
            .unwrap();
        input_sender
            .send(Pipeline::from_iter(vec![3, 4, 5]))
            .await
            .unwrap();

        drop(input_sender);

        let mut results = vec![];

        while let Some(output) = output_receiver.recv().await {
            results.push(output);
        }

        // order is not preserved
        assert_eq!(
            results.into_iter().collect::<HashSet<_>>(),
            HashSet::from([1, 2, 3, 3, 4, 5])
        );
    }
}
