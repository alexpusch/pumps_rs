use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::Pump;

pub struct TakePump {
    pub(crate) n: usize,
}

impl<In> Pump<In, In> for TakePump
where
    In: Send + 'static,
{
    fn spawn(self, mut input_receiver: Receiver<In>) -> (Receiver<In>, JoinHandle<()>) {
        let (output_sender, output_receiver) = mpsc::channel(1);

        let h = tokio::spawn(async move {
            let mut i = 0;
            while let Some(input) = input_receiver.recv().await {
                if i < self.n {
                    if let Err(_e) = output_sender.send(input).await {
                        break;
                    }
                } else {
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
    async fn take_works() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver).take(2).build();

        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, None);

        drop(input_sender);
        join_handle.await.unwrap();
    }
}
