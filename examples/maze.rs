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

#[tokio::main]
async fn main() {
    // A simple maze (string) to visualize the current position and obstacles
    let maze = "-#---#----#------#-----";

    let (mut output_receiver, _h) = Pipeline::from_iter(1..=25)
        .map(
            // Make some moves
            |x| async move { x % 3 },
            Concurrency::concurrent_unordered(3),
        )
        .pump(StatePump {
            init_value: 0,
            // Move forward by the number of steps
            transition_fn: move |step: i32, position: i32| async move {
                let next_position = position + step;
                if maze.chars().nth(next_position as usize) == Some('#') {
                    // Stay if there is an obstacle
                    position
                } else {
                    next_position
                }
            },
        })
        .build();

    while let Some(output) = output_receiver.recv().await {
        // Mark the current position in the maze as "*"
        let result = maze
            .chars()
            .enumerate()
            .map(|(i, c)| if i == output as usize { '*' } else { c })
            .collect::<String>();

        println!("{result}");
    }
}
