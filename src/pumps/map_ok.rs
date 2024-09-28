use std::future::Future;

use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::{concurrency::Concurrency, concurrency_base, Pump};

pub struct MapOkPump<F> {
    pub(crate) map_fn: F,
    pub(crate) concurrency: Concurrency,
}

impl<In, InE, Out, F, Fut> Pump<Result<In, InE>, Result<Out, InE>> for MapOkPump<F>
where
    F: Fn(In) -> Fut + Send + 'static,
    Fut: Future<Output = Out> + Send,
    In: Send + 'static,
    InE: Send + 'static,
    Out: Send + 'static,
{
    fn spawn(
        self,
        mut input_receiver: Receiver<Result<In, InE>>,
    ) -> (Receiver<Result<Out, InE>>, JoinHandle<()>) {
        concurrency_base! {
            input_receiver = input_receiver;
            concurrency = self.concurrency;


            on_input(input, in_progress) => {
                match input {
                    Ok(input) => {
                        let fut = (self.map_fn)(input);
                        in_progress.push_back(fut);
                    },
                    Err(e) => {
                        if let Err(_e) = output_sender.send(Err(e)).await {
                            break;
                        }
                    }
                }
            },
            on_progress(output, output_sender) => {
                if let Err(_e) = output_sender.send(Ok(output)).await {
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
    async fn map_ok_works() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .map_ok(|x| async move { x * 2 }, Default::default())
            .build();

        input_sender.send(Ok(1)).await.unwrap();
        input_sender.send(Err("oh no")).await.unwrap();
        input_sender.send(Ok(3)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(Ok(2)));
        assert_eq!(output_receiver.recv().await, Some(Err("oh no")));
        assert_eq!(output_receiver.recv().await, Some(Ok(6)));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, None);

        assert!(matches!(join_handle.await, Ok(())));
    }
}
