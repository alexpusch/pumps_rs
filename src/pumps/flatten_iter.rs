use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::Pump;

pub struct FlattenIterPump {}

impl<Out, In: IntoIterator<Item = Out>> Pump<In, Out> for FlattenIterPump
where
    In: Send + 'static,
    Out: Send + 'static,
    <In as IntoIterator>::IntoIter: Send,
{
    fn spawn(self, mut input_receiver: Receiver<In>) -> (Receiver<Out>, JoinHandle<()>) {
        let (output_sender, output_receiver) = tokio::sync::mpsc::channel(1);
        let h = tokio::spawn(async move {
            'outer: while let Some(input) = input_receiver.recv().await {
                for i in input.into_iter() {
                    if output_sender.send(i).await.is_err() {
                        break 'outer;
                    }
                }
            }
        });

        (output_receiver, h)
    }
}

#[cfg(test)]
mod tests {
    use super::FlattenIterPump;
    use crate::Pipeline;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn flatten_iter_works() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .pump(FlattenIterPump {})
            .build();

        input_sender.send(vec![1, 2, 3]).await.unwrap();
        input_sender.send(vec![4, 5]).await.unwrap();
        input_sender.send(vec![]).await.unwrap();
        input_sender.send(vec![6]).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(3));
        assert_eq!(output_receiver.recv().await, Some(4));
        assert_eq!(output_receiver.recv().await, Some(5));
        assert_eq!(output_receiver.recv().await, Some(6));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, None);
        join_handle.await.unwrap();
    }
}
