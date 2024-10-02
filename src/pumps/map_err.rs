use std::future::Future;

use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::{concurrency::Concurrency, concurrency_base, Pump};

pub struct MapErrPump<F> {
    pub(crate) map_fn: F,
    pub(crate) concurrency: Concurrency,
}

impl<InOk, InErr, OutErr, F, Fut> Pump<Result<InOk, InErr>, Result<InOk, OutErr>> for MapErrPump<F>
where
    F: FnMut(InErr) -> Fut + Send + 'static,
    Fut: Future<Output = OutErr> + Send,
    InOk: Send + 'static,
    InErr: Send + 'static,
    OutErr: Send + 'static,
{
    fn spawn(
        mut self,
        mut input_receiver: Receiver<Result<InOk, InErr>>,
    ) -> (Receiver<Result<InOk, OutErr>>, JoinHandle<()>) {
        concurrency_base! {
            input_receiver = input_receiver;
            concurrency = self.concurrency;


            on_input(input, in_progress) => {
                match input {
                    Ok(input) => {
                      if let Err(_e) = output_sender.send(Ok(input)).await {
                        break;
                    }


                    },
                    Err(e) => {
                      let fut = (self.map_fn)(e);
                      in_progress.push_back(fut);
                    }
                }
            },
            on_progress(output, output_sender) => {
                if let Err(_e) = output_sender.send(Err(output)).await {
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
    async fn map_err_works() {
        let (input_sender, input_receiver) = mpsc::channel(100);

        let (mut output_receiver, join_handle) = Pipeline::from(input_receiver)
            .map_err(|x| async move { format!("{x}!!!") }, Default::default())
            .build();

        input_sender.send(Ok(1)).await.unwrap();
        input_sender.send(Err("oh no")).await.unwrap();
        input_sender.send(Ok(3)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(Ok(1)));
        assert_eq!(
            output_receiver.recv().await,
            Some(Err("oh no!!!".to_string()))
        );
        assert_eq!(output_receiver.recv().await, Some(Ok(3)));

        drop(input_sender);
        assert_eq!(output_receiver.recv().await, None);

        assert!(matches!(join_handle.await, Ok(())));
    }
}
