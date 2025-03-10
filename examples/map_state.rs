use pumps::{Concurrency, Pipeline, Pump};
use std::future::Future;
use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

pub struct StatePump<Out, F> {
    pub(crate) init_value: Out,
    pub(crate) transition_fn: F,
}

impl<In, Out, F, Fut> Pump<In, Out> for StatePump<Out, F>
where
    F: FnMut(In, Out) -> Fut + Send + 'static,
    Fut: Future<Output = Out> + Send,
    In: Send + 'static,
    Out: Send + Clone + 'static,
{
    fn spawn(mut self, mut input_receiver: Receiver<In>) -> (Receiver<Out>, JoinHandle<()>) {
        let (output_sender, output_receiver) = mpsc::channel(1);

        let h = tokio::spawn(async move {
            let mut current_state = self.init_value;
            while let Some(input) = input_receiver.recv().await {
                let next = (self.transition_fn)(input, current_state).await;
                if let Err(_e) = output_sender.send(next.clone()).await {
                    break;
                }

                current_state = next;
            }
        });

        (output_receiver, h)
    }
}

// tokio async main
#[tokio::main]
async fn main() {
    let maze = "-----------------------";
    let input = (1..=20).collect::<Vec<_>>();
    let (mut output_receiver, _h) = Pipeline::from_iter(input)
        .map(
            // Move 1 step to the right if the number is even
            |x| async move { x % 2 },
            Concurrency::concurrent_unordered(3),
        )
        .pump(StatePump {
            init_value: 0,
            // Move forward by the number of steps
            transition_fn: |step: i32, position: i32| async move { step + position },
        })
        .build();

    while let Some(output) = output_receiver.recv().await {
        // Mark the current position in the maze as "*"
        let output = maze
            .chars()
            .enumerate()
            .map(|(i, c)| if i == output as usize { '*' } else { c })
            .collect::<String>();

        println!("{output}");
    }
}
