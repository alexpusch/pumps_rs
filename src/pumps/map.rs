use std::future::Future;

use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::{concurrency::Concurrency, concurrency_base, Pump};

pub struct MapPump<F> {
    pub(crate) map_fn: F,
    pub(crate) concurrency: Concurrency,
}

impl<In, Out, F, Fut> Pump<In, Out> for MapPump<F>
where
    F: FnMut(In) -> Fut + Send + 'static,
    Fut: Future<Output = Out> + Send,
    In: Send + 'static,
    Out: Send + 'static,
{
    fn spawn(mut self, mut input_receiver: Receiver<In>) -> (Receiver<Out>, JoinHandle<()>) {
        concurrency_base! {
            input_receiver = input_receiver;
            concurrency = self.concurrency;


            on_input(input, in_progress) => {
                let fut = (self.map_fn)(input);
                in_progress.push_back(fut);
            },
            on_progress(output, output_sender) => {
                if let Err(_e) = output_sender.send(output).await {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use tokio::sync::mpsc;

    use crate::Pipeline;

    #[tokio::test]
    async fn map_works() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .map(|x| async move { x * 2 }, Default::default())
            .build();

        input_sender.send(1).await.unwrap();
        input_sender.send(2).await.unwrap();
        input_sender.send(3).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(4));
        assert_eq!(output_receiver.recv().await, Some(6));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, None);

        assert!(matches!(join_handle.await, Ok(())));
    }
}
