use pumps::{Concurrency, Pipeline, Pump};
use std::future::Future;
use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

pub struct FoldPump<Out, F> {
    pub(crate) init: Out,
    pub(crate) fold_fn: F,
}

impl<In, Out, F, Fut> Pump<In, Out> for FoldPump<Out, F>
where
    F: FnMut(In, Out) -> Fut + Send + 'static,
    Fut: Future<Output = Out> + Send,
    In: Send + 'static,
    Out: Send + Clone + 'static,
{
    fn spawn(mut self, mut input_receiver: Receiver<In>) -> (Receiver<Out>, JoinHandle<()>) {
        let (output_sender, output_receiver) = mpsc::channel(1);

        let h = tokio::spawn(async move {
            let mut init = self.init;
            while let Some(input) = input_receiver.recv().await {
                let next = (self.fold_fn)(input, init).await;
                if let Err(_e) = output_sender.send(next.clone()).await {
                    break;
                }

                init = next;
            }
        });

        (output_receiver, h)
    }
}

// tokio async main
#[tokio::main]
async fn main() {
    let (mut output_receiver, _h) = Pipeline::from_iter(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        .map(|x| async move { x - 1 }, Concurrency::concurrent_ordered(3))
        .pump(FoldPump {
            init: 0,
            fold_fn: |a: i32, b: i32| async move { a + b },
        })
        .build();

    while let Some(output) = output_receiver.recv().await {
        println!("{output}");
    }
}
