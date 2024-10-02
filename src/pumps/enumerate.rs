use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::Pump;

pub struct EnumeratePump;

impl<In> Pump<In, (usize, In)> for EnumeratePump
where
    In: Send + 'static,
{
    fn spawn(self, mut input_receiver: Receiver<In>) -> (Receiver<(usize, In)>, JoinHandle<()>) {
        let (output_sender, output_receiver) = mpsc::channel(1);

        let h = tokio::spawn(async move {
            let mut i = 0;
            while let Some(input) = input_receiver.recv().await {
                if let Err(_e) = output_sender.send((i, input)).await {
                    break;
                }
                i += 1;
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
    async fn enumerate_works() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver).enumerate().build();

        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some((0, 1)));
        assert_eq!(output_receiver.recv().await, Some((1, 2)));
        assert_eq!(output_receiver.recv().await, Some((2, 3)));

        drop(input_sender);
        join_handle.await.unwrap();
    }
}
