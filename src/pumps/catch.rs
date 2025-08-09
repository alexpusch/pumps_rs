use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::Pump;

pub struct CatchPump<E>
where
    E: Send + 'static,
{
    pub(crate) err_channel: tokio::sync::mpsc::Sender<E>,
    pub(crate) abort_on_error: bool,
}

impl<E, In> Pump<Result<In, E>, In> for CatchPump<E>
where
    E: Send + 'static,
    In: Send + 'static,
{
    fn spawn(self, mut input_rx: Receiver<Result<In, E>>) -> (Receiver<In>, JoinHandle<()>) {
        let (output_tx, output_rx) = tokio::sync::mpsc::channel(1);

        let h = tokio::spawn(async move {
            while let Some(input) = input_rx.recv().await {
                match input {
                    Err(e) => {
                        // Ignore send errors, we should not stop the
                        // flow of data if the error catcher closes
                        let _ = self.err_channel.send(e).await;

                        if self.abort_on_error {
                            break;
                        }
                    }

                    Ok(x) => {
                        if let Err(_) = output_tx.send(x).await {
                            // Collect remaining errors
                            input_rx.close();
                            while let Ok(x) = input_rx.try_recv() {
                                if let Err(x) = x {
                                    let _ = self.err_channel.send(x).await;

                                    // Only send one error if `abort_on_error` is true
                                    if self.abort_on_error {
                                        break;
                                    }
                                }
                            }

                            break;
                        }
                    }
                }
            }
        });

        (output_rx, h)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::{self, error::TryRecvError};

    #[tokio::test]
    async fn catch_works() {
        let (input_tx, input_rx) = mpsc::channel(10);
        let (error_tx, mut error_rx) = mpsc::channel::<()>(10);

        let catch_pump = CatchPump {
            err_channel: error_tx,
            abort_on_error: false,
        };

        let (mut output_rx, join_handle) = catch_pump.spawn(input_rx);

        input_tx.send(Ok(1)).await.unwrap();
        input_tx.send(Ok(2)).await.unwrap();
        input_tx.send(Ok(3)).await.unwrap();
        drop(input_tx);

        assert_eq!(output_rx.recv().await, Some(1));
        assert_eq!(output_rx.recv().await, Some(2));
        assert_eq!(output_rx.recv().await, Some(3));
        assert_eq!(output_rx.recv().await, None);
        assert_eq!(error_rx.try_recv().unwrap_err(), TryRecvError::Disconnected);

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn catch_catches_errors_without_abort() {
        let (input_tx, input_rx) = mpsc::channel(10);
        let (error_tx, mut error_rx) = mpsc::channel(10);

        let catch_pump = CatchPump {
            err_channel: error_tx,
            abort_on_error: false,
        };

        let (mut output_rx, join_handle) = catch_pump.spawn(input_rx);

        input_tx.send(Err("error1")).await.unwrap();
        input_tx.send(Ok(42)).await.unwrap();
        input_tx.send(Err("error2")).await.unwrap();
        input_tx.send(Ok(99)).await.unwrap();
        drop(input_tx);

        assert_eq!(output_rx.recv().await, Some(42));
        assert_eq!(output_rx.recv().await, Some(99));
        assert_eq!(output_rx.recv().await, None);

        assert_eq!(error_rx.recv().await, Some("error1"));
        assert_eq!(error_rx.recv().await, Some("error2"));
        assert_eq!(error_rx.try_recv().unwrap_err(), TryRecvError::Disconnected);

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn catch_aborts_on_error() {
        let (input_tx, input_rx) = mpsc::channel(10);
        let (error_tx, mut error_rx) = mpsc::channel(10);

        let catch_pump = CatchPump {
            err_channel: error_tx,
            abort_on_error: true,
        };

        let (mut output_rx, join_handle) = catch_pump.spawn(input_rx);

        input_tx.send(Ok(1)).await.unwrap();
        input_tx.send(Err("fatal_error")).await.unwrap();
        input_tx.send(Ok(2)).await.unwrap(); // This shouldn't be processed
        drop(input_tx);

        assert_eq!(output_rx.recv().await, Some(1));
        assert_eq!(output_rx.recv().await, None); // Channel closed after error

        assert_eq!(error_rx.recv().await, Some("fatal_error"));
        assert_eq!(error_rx.try_recv().unwrap_err(), TryRecvError::Disconnected);

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn catch_empty_input() {
        let (input_tx, input_rx) = mpsc::channel::<Result<(), ()>>(10);
        let (error_tx, mut error_rx) = mpsc::channel::<()>(10);

        let catch_pump = CatchPump {
            err_channel: error_tx,
            abort_on_error: false,
        };

        let (mut output_rx, join_handle) = catch_pump.spawn(input_rx);

        drop(input_tx);

        assert_eq!(output_rx.recv().await, None);
        assert_eq!(error_rx.try_recv().unwrap_err(), TryRecvError::Disconnected);

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn catch_output_receiver_dropped() {
        let (input_tx, input_rx) = mpsc::channel(10);
        let (error_tx, mut error_rx) = mpsc::channel(10);

        let catch_pump = CatchPump {
            err_channel: error_tx,
            abort_on_error: false,
        };

        let (output_rx, join_handle) = catch_pump.spawn(input_rx);
        drop(output_rx);

        input_tx.send(Ok(1)).await.unwrap();
        input_tx.send(Err("error1")).await.unwrap();
        input_tx.send(Err("error2")).await.unwrap();
        drop(input_tx);

        // We should get all errors still in the channel
        assert_eq!(error_rx.recv().await, Some("error1"));
        assert_eq!(error_rx.recv().await, Some("error2"));
        assert_eq!(error_rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn catch_abort_receiver_dropped() {
        let (input_tx, input_rx) = mpsc::channel(10);
        let (error_tx, mut error_rx) = mpsc::channel(10);

        let catch_pump = CatchPump {
            err_channel: error_tx,
            abort_on_error: true,
        };

        let (output_rx, join_handle) = catch_pump.spawn(input_rx);
        drop(output_rx);

        input_tx.send(Ok(1)).await.unwrap();
        input_tx.send(Err("error1")).await.unwrap();
        input_tx.send(Err("error2")).await.unwrap();
        input_tx.send(Err("error3")).await.unwrap();
        drop(input_tx);

        // If `abort_on_error` is true, we should send an error at most once.
        assert_eq!(error_rx.recv().await, Some("error1"));
        assert_eq!(error_rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        join_handle.await.unwrap();
    }
}
