use std::future::Future;

use futures::FutureExt;
use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::Pump;

pub struct BatchWhileWithExpiryPump<F, State> {
    pub(crate) state_init: State,
    pub(crate) while_fn: F,
}

impl<T, F, Fut, State> Pump<T, Vec<T>> for BatchWhileWithExpiryPump<F, State>
where
    F: FnMut(State, &T) -> Option<(State, Fut)> + Send + 'static,
    Fut: Future<Output = ()> + Send,
    State: Send + Clone + 'static,
    T: Send + 'static,
{
    fn spawn(mut self, mut input_receiver: Receiver<T>) -> (Receiver<Vec<T>>, JoinHandle<()>) {
        let (output_sender, output_receiver) = mpsc::channel(1);

        let h = tokio::spawn(async move {
            let mut batch = Vec::new();
            let mut current_state = self.state_init.clone();

            let mut expiry_fut = futures::future::pending().boxed();

            loop {
                tokio::select! {
                    biased;

                    _ = expiry_fut => {
                        expiry_fut = futures::future::pending().boxed();
                        current_state = self.state_init.clone();

                        if !batch.is_empty() {
                            let batch = std::mem::take(&mut batch);
                            if let Err(_e) = output_sender.send(batch).await {
                                break;
                            }
                        }
                    }

                    input = input_receiver.recv() => {
                        let Some(input) = input else {
                            break;
                        };

                        let res = (self.while_fn)(current_state, &input);
                        batch.push(input);

                        if let Some((new_state, new_state_fut)) = res {
                            current_state = new_state;
                            expiry_fut = new_state_fut.boxed();
                        } else {
                            current_state = self.state_init.clone();
                            expiry_fut = futures::future::pending().boxed();

                            let batch = std::mem::take(&mut batch);
                            if let Err(_e) = output_sender.send(batch).await {
                                break;
                            }
                        }
                    }

                }
            }

            if !batch.is_empty() {
                // Try to send the remaining batch, in case the output channel is still alive.
                let _ = output_sender.send(batch).await;
            }
        });

        (output_receiver, h)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use crate::{test_utils::wait_for_len, Pipeline};

    #[tokio::test]
    async fn batch_while_with_expiry_batches_while_predicate_is_true() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .batch_while_with_expiry(0, |state, x| {
                let sum = state + x;

                (sum < 10).then_some((sum, tokio::time::sleep(Duration::from_millis(100))))
            })
            .build();

        input_sender.send(4).await.unwrap();
        input_sender.send(5).await.unwrap();
        assert_eq!(wait_for_len(&output_receiver).await, 0);

        input_sender.send(2).await.unwrap(); // reached 10. should batch
        input_sender.send(3).await.unwrap();
        input_sender.send(3).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(vec![4, 5, 2]));

        input_sender.send(3).await.unwrap();
        assert_eq!(wait_for_len(&output_receiver).await, 0);

        input_sender.send(2).await.unwrap();
        input_sender.send(5).await.unwrap();
        assert_eq!(output_receiver.recv().await, Some(vec![3, 3, 3, 2]));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, Some(vec![5])); // the last batch

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn batch_while_with_expiry_emits_batch_on_expire() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let expiry = Duration::from_millis(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .batch_while_with_expiry(0, move |state, x| {
                let sum = state + x;

                (sum < 10).then_some((sum, tokio::time::sleep(expiry)))
            })
            .build();

        input_sender.send(4).await.unwrap();
        input_sender.send(5).await.unwrap();
        assert_eq!(wait_for_len(&output_receiver).await, 0);
        tokio::time::sleep(expiry * 2).await;

        assert_eq!(output_receiver.recv().await, Some(vec![4, 5]));

        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();
        input_sender.send(6).await.unwrap();
        input_sender.send(5).await.unwrap();
        assert_eq!(output_receiver.recv().await, Some(vec![2, 3, 6]));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, Some(vec![5])); // the last batch
        assert_eq!(output_receiver.recv().await, None);
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn batch_while_batches_while_predicate_is_true() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .batch_while(0, |state, x| {
                let sum = state + x;

                (sum < 10).then_some(sum)
            })
            .build();

        input_sender.send(4).await.unwrap();
        input_sender.send(5).await.unwrap();
        assert_eq!(wait_for_len(&output_receiver).await, 0);

        input_sender.send(2).await.unwrap(); // reached 10. should batch
        input_sender.send(3).await.unwrap();
        input_sender.send(3).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(vec![4, 5, 2]));

        input_sender.send(3).await.unwrap();
        assert_eq!(wait_for_len(&output_receiver).await, 0);

        input_sender.send(2).await.unwrap();
        input_sender.send(5).await.unwrap();
        assert_eq!(output_receiver.recv().await, Some(vec![3, 3, 3, 2]));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, Some(vec![5])); // the last batch

        join_handle.await.unwrap();
    }
}
