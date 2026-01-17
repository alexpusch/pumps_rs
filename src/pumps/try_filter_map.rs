use std::future::Future;

use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::{concurrency::Concurrency, concurrency_base, Pump};

pub struct TryFilterMapPump<F> {
    pub(crate) map_fn: F,
    pub(crate) concurrency: Concurrency,
}

impl<InOk, InErr, OutOk, F, Fut> Pump<Result<InOk, InErr>, Result<OutOk, InErr>>
    for TryFilterMapPump<F>
where
    F: FnMut(InOk) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Option<OutOk>, InErr>> + Send,
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
                match output {
                    Ok(Some(output)) => {
                        if let Err(_e) = output_sender.send(Ok(output)).await {
                            break;
                        }
                    },
                     Ok(None) => {},
                     Err(e) => {
                        if let Err(_e) = output_sender.send(Err(e)).await {
                            break;
                        }
                     }
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
    async fn try_filter_map_works() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .try_filter_map(
                |x| async move {
                    if x % 2 == 0 {
                        Ok(Some(x))
                    } else if x % 3 == 0 {
                        Ok(None)
                    } else {
                        Err("odd and not divisible by 3")
                    }
                },
                Default::default(),
            )
            .build();

        input_sender.send(Ok(2)).await.unwrap(); // driven by map_fn: Ok(Some(2)) -> Ok(2)
        input_sender.send(Ok(3)).await.unwrap(); // driven by map_fn: Ok(None) -> filtered
        input_sender.send(Ok(5)).await.unwrap(); // driven by map_fn: Err -> Err
        input_sender.send(Err("input error")).await.unwrap(); // pass through: Err

        assert_eq!(output_receiver.recv().await, Some(Ok(2)));
        assert_eq!(
            output_receiver.recv().await,
            Some(Err("odd and not divisible by 3"))
        );
        assert_eq!(output_receiver.recv().await, Some(Err("input error")));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, None);

        assert!(matches!(join_handle.await, Ok(())));
    }
}
