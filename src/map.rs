use futures::{channel::mpsc::Receiver, SinkExt, StreamExt};

use crate::pumps::Pump;

pub struct MapPump<F> {
    pub(crate) map_fn: F,
}

impl<In, Out, F> Pump<In, Out> for MapPump<F>
where
    F: Fn(In) -> Out + Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
{
    fn spawn(self, mut input_receiver: Receiver<In>) -> Receiver<Out> {
        let (mut output_sender, output_receiver) = futures::channel::mpsc::channel(1); // what is the size?

        let map_fn = self.map_fn;
        tokio::spawn(async move {
            while let Some(input) = input_receiver.next().await {
                let output = map_fn(input);

                if let Err(_e) = output_sender.send(output).await {
                    break;
                }
            }
        });

        output_receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_map_pump() {
        let (mut input_sender, input_receiver) = futures::channel::mpsc::channel(100);
        let pump = MapPump {
            map_fn: |x: i32| x * 2,
        };

        let output_receiver = pump.spawn(input_receiver);

        let mut output_receiver = output_receiver;
        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();

        assert_eq!(output_receiver.next().await, Some(2));
        assert_eq!(output_receiver.next().await, Some(4));
        assert_eq!(output_receiver.next().await, Some(6));

        drop(input_sender);
        assert_eq!(output_receiver.next().await, None);
    }
}
