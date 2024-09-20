use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::pumps::{Pipeline, Pump};

pub struct FlattenConcurrency {
    pub backpressure: usize,
    pub preserve_order: bool,
}

impl FlattenConcurrency {
    pub fn ordered() -> Self {
        Self {
            backpressure: 1,
            preserve_order: true,
        }
    }

    pub fn unordered() -> Self {
        Self {
            backpressure: 1,
            preserve_order: false,
        }
    }

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
