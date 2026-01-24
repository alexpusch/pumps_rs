use std::future::Future;

use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::{concurrency::Concurrency, concurrency_base, Pump};

pub struct AndThenPump<F> {
    pub(crate) map_fn: F,
    pub(crate) concurrency: Concurrency,
}

impl<InOk, InErr, OutOk, F, Fut> Pump<Result<InOk, InErr>, Result<OutOk, InErr>> for AndThenPump<F>
where
    F: FnMut(InOk) -> Fut + Send + 'static,
    Fut: Future<Output = Result<OutOk, InErr>> + Send,
    InOk: Send + 'static,
    InErr: Send + 'static,
    OutOk: Send + 'static,
{
    fn spawn(
        mut self,
        mut input_receiver: Receiver<Result<InOk, InErr>>,
    ) -> (Receiver<Result<OutOk, InErr>>, JoinHandle<()>) {
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
                if let Err(_e) = output_sender.send(output).await {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Pipeline;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn and_then_works() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .and_then(
                |x| async move {
                    if x % 2 == 0 {
                        Ok(x)
                    } else {
                        Err("odd")
                    }
                },
                Default::default(),
            )
            .build();

        input_sender.send(Ok(1)).await.unwrap();
        input_sender.send(Err("oh no")).await.unwrap();
        input_sender.send(Ok(2)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(Err("odd")));
        assert_eq!(output_receiver.recv().await, Some(Err("oh no")));
        assert_eq!(output_receiver.recv().await, Some(Ok(2)));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, None);

        assert!(matches!(join_handle.await, Ok(())));
    }
}
