use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::Pump;

pub struct BatchPump {
    pub(crate) n: usize,
}

impl<T> Pump<T, Vec<T>> for BatchPump
where
    T: Send + 'static,
{
    fn spawn(self, mut input_receiver: Receiver<T>) -> (Receiver<Vec<T>>, JoinHandle<()>) {
        let (output_sender, output_receiver) = mpsc::channel(1);

        let h = tokio::spawn(async move {
            let mut batch = Vec::with_capacity(self.n);
            while let Some(input) = input_receiver.recv().await {
                batch.push(input);
                if batch.len() == self.n {
                    let batch = std::mem::replace(&mut batch, Vec::with_capacity(self.n));
                    if let Err(_e) = output_sender.send(batch).await {
                        break;
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
    use tokio::sync::mpsc;

    use crate::Pipeline;

    #[tokio::test]
    async fn batch_batches_n_items() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver).batch(2).build();

        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();
        input_sender.send(4).await.unwrap();
        input_sender.send(5).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(vec![1, 2]));
        assert_eq!(output_receiver.recv().await, Some(vec![3, 4]));
        assert_eq!(output_receiver.len(), 0); // output_receiver.recv() waits forever
        drop(input_sender);
        assert_eq!(output_receiver.recv().await, Some(vec![5]));
        join_handle.await.unwrap();
    }
}
