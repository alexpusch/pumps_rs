use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::Pump;

pub struct BatchPump<T> {
    pub(crate) n: usize,
    batch: Vec<T>,
}

impl<T> BatchPump<T> {
    pub fn new(n: usize) -> Self {
        Self {
            n,
            batch: Vec::with_capacity(n),
        }
    }
}

impl<T> Pump<T, Vec<T>> for BatchPump<T>
where
    T: Send + 'static,
{
    fn spawn(mut self, mut input_receiver: Receiver<T>) -> (Receiver<Vec<T>>, JoinHandle<()>) {
        let (output_sender, output_receiver) = mpsc::channel(1);

        let h = tokio::spawn(async move {
            while let Some(input) = input_receiver.recv().await {
                self.batch.push(input);
                if self.batch.len() >= self.n {
                    let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(self.n));
                    if let Err(_e) = output_sender.send(batch).await {
                        break;
                    }
                }
            }
            if !self.batch.is_empty() {
                let _ = output_sender.send(self.batch).await;
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
    async fn batch_works() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver).batch(2).build();

        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();
        input_sender.send(4).await.unwrap();
        input_sender.send(5).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(vec![1, 2]));
        assert_eq!(output_receiver.recv().await, Some(vec![3, 4]));
        // waits forever... assert_eq!(output_receiver.recv().await, Some(vec![5]));
        drop(input_sender);
        assert_eq!(output_receiver.recv().await, Some(vec![5]));
        join_handle.await.unwrap();
    }
}
