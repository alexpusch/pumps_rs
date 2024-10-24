use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::Pump;

pub struct Backpressure {
    pub(crate) n: usize,
}

impl<In> Pump<In, In> for Backpressure
where
    In: Send + 'static,
{
    fn spawn(self, mut input_receiver: Receiver<In>) -> (Receiver<In>, JoinHandle<()>) {
        // pipe channel(?) to channel(n)
        // for example, if ? = 1, we'll get buffering of n elements
        let (output_sender, output_receiver) = mpsc::channel::<In>(self.n);

        let h = tokio::spawn(async move {
            while let Some(input) = input_receiver.recv().await {
                if let Err(_) = output_sender.send(input).await {
                    break;
                }
            }
        });

        (output_receiver, h)
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn backpressure_buffers_1_input_at_a_time() {
        let (input_sender, input_receiver) = mpsc::channel(1);

        let (mut output_receiver, join_handle) = crate::Pipeline::from(input_receiver)
            .backpressure(3)
            .build();

        input_sender.send(1).await.unwrap();
        assert_eq!(output_receiver.recv().await, Some(1));

        input_sender.send(2).await.unwrap();
        assert_eq!(output_receiver.recv().await, Some(2));

        input_sender.send(3).await.unwrap();
        assert_eq!(output_receiver.recv().await, Some(3));

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn backpressure_buffers_less_from_given_n() {
        let (input_sender, input_receiver) = mpsc::channel(1);

        let (mut output_receiver, join_handle) = crate::Pipeline::from(input_receiver)
            .backpressure(3)
            .build();

        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn backpressure_buffers_n_inputs_while_not_consumed() {
        let (input_sender, input_receiver) = mpsc::channel(1);

        let (mut output_receiver, join_handle) = crate::Pipeline::from(input_receiver)
            .backpressure(2)
            .build();

        async fn wait_for_capacity(sender: &mpsc::Sender<i32>) -> usize {
            tokio::time::sleep(Duration::from_millis(20)).await;
            sender.capacity()
        }

        input_sender.send(1).await.unwrap(); // this arrives to the output channel
        assert_eq!(wait_for_capacity(&input_sender).await, 1);

        input_sender.send(2).await.unwrap(); // buffers
        assert_eq!(wait_for_capacity(&input_sender).await, 1);

        input_sender.send(3).await.unwrap(); // buffers
        assert_eq!(wait_for_capacity(&input_sender).await, 1);

        input_sender.send(4).await.unwrap(); // stays in input_receiver
        assert_eq!(wait_for_capacity(&input_sender).await, 0);

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(3));
        assert_eq!(output_receiver.recv().await, Some(4));

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn backpressure_buffers_vecates_when_consumed() {
        let (input_sender, input_receiver) = mpsc::channel(1);

        let (mut output_receiver, join_handle) = crate::Pipeline::from(input_receiver)
            .backpressure(2)
            .build();

        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();
        input_sender.send(4).await.unwrap(); // buffer is full

        assert_eq!(output_receiver.recv().await, Some(1));

        input_sender.send(5).await.unwrap(); // can send thanks to the consumed 1
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(3));
        assert_eq!(output_receiver.recv().await, Some(4));
        assert_eq!(output_receiver.recv().await, Some(5));

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);

        join_handle.await.unwrap();
    }
}
