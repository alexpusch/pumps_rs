use tokio::{sync::mpsc::Receiver, task::JoinHandle};

/// A `Pump` is a component that data flows through, processed, and flows out.
/// It is a wrapper around an input receiver, a task and an output sender.
/// ```
pub trait Pump<In, Out> {
    /// Spawns the pumps task and returns the output receiver and the tasks join handle
    fn spawn(self, input_receiver: Receiver<In>) -> (Receiver<Out>, JoinHandle<()>);
}
