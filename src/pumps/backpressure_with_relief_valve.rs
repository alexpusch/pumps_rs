use std::collections::VecDeque;

use futures::FutureExt;
use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::Pump;

pub struct BackpressureWithReliefValve {
    pub(crate) n: usize,
}

impl<In> Pump<In, In> for BackpressureWithReliefValve
where
    In: Send + 'static,
{
    fn spawn(self, mut input_receiver: Receiver<In>) -> (Receiver<In>, JoinHandle<()>) {
        let (output_sender, output_receiver) = mpsc::channel::<In>(1);

        let mut buffer = VecDeque::with_capacity(self.n);

        let h = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    output_res = output_sender.reserve().then(|_| {
                        let front = buffer.pop_front().expect("buffer is not empty");
                        output_sender.send(front)
                    }), if !buffer.is_empty() => {
                        if output_res.is_err() {
                            break;
                        }
                    }

                    input_res = input_receiver.recv() => {
                        match input_res {
                            Some(input) =>  {
                                buffer.push_back(input);
                                if buffer.len() >= self.n {
                                    buffer.pop_front();
                                }
                            },
                            None => break,
                        }
                    },
                }
            }
        });

        (output_receiver, h)
    }
}

#[cfg(test)]
mod test {
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn backpressure_with_relief_valve_drops_old_inputs_from_the_start() {
        let (input_sender, input_receiver) = mpsc::channel(1);

        let (mut output_receiver, join_handle) = crate::Pipeline::from(input_receiver)
            .backpressure_with_relief_valve(2)
            .build();

        input_sender.send(1).await.unwrap(); // goes to output channel
        input_sender.send(2).await.unwrap(); // buffers, than dropped
        input_sender.send(3).await.unwrap(); // buffers
        input_sender.send(4).await.unwrap(); // buffers

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(3));
        assert_eq!(output_receiver.recv().await, Some(4));

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn backpressure_with_relief_valve_drops_old_inputs() {
        let (input_sender, input_receiver) = mpsc::channel(1);

        let (mut output_receiver, join_handle) = crate::Pipeline::from(input_receiver)
            .backpressure_with_relief_valve(2)
            .build();

        input_sender.send(1).await.unwrap();
        assert_eq!(output_receiver.recv().await, Some(1));

        input_sender.send(2).await.unwrap(); // goes to output channel
        input_sender.send(3).await.unwrap(); // buffers then dropped
        input_sender.send(4).await.unwrap(); // buffers then dropped
        input_sender.send(5).await.unwrap(); // buffers
        input_sender.send(6).await.unwrap(); // buffers

        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(5));
        assert_eq!(output_receiver.recv().await, Some(6));

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn backpressure_with_relief_valve_consumer_keeps_up() {
        let (input_sender, input_receiver) = mpsc::channel(1);

        let (mut output_receiver, join_handle) = crate::Pipeline::from(input_receiver)
            .backpressure_with_relief_valve(2)
            .build();

        input_sender.send(1).await.unwrap(); // goes to output channel
        input_sender.send(2).await.unwrap(); // buffers
        input_sender.send(3).await.unwrap(); // buffers

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2)); // 3 is in output channel now

        input_sender.send(4).await.unwrap(); // buffers
        input_sender.send(5).await.unwrap(); // buffers

        assert_eq!(output_receiver.recv().await, Some(3));
        assert_eq!(output_receiver.recv().await, Some(4));
        assert_eq!(output_receiver.recv().await, Some(5));

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);

        join_handle.await.unwrap();
    }
}
