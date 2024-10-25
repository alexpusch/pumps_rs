//! Eager streams for Rust. If a stream allows water to flow down the hill, a pump forces it up.
//!
//! This crate offers an alternative approach for rust async pipelining.
//!
//! Main features:
//!
//! - Designed with common async pipelining needs in heart
//! - Explicit concurrency, ordering, and backpressure control
//! - Eager - work is done before downstream methods consumes it
//! - builds on top of Rust async tools as tasks and channels.
//! - For now only supports the Tokio async runtime
//!
//! Example:
//!
//! ```rust
//! use pumps::{Pipeline, Concurrency};
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # async fn get_json(url: String) -> String { url }
//! # async fn download_heavy_resource(json: String) -> String { json }
//! # async fn run_algorithm(json: String) -> Option<String> { Some(json) }
//! # async fn save_to_db(json: String) -> String { json }
//! # let urls:Vec<String> = Vec::new();
//! let (mut output_receiver, join_handle) = Pipeline::from_iter(urls)
//!     .map(get_json, Concurrency::concurrent_ordered(5))
//!     .backpressure(100)
//!     .map(download_heavy_resource, Concurrency::serial())
//!     .filter_map(run_algorithm, Concurrency::concurrent_unordered(5))
//!     .map(save_to_db, Concurrency::concurrent_unordered(100))
//!     .build();
//!
//! while let Some(output) = output_receiver.recv().await {
//!     println!("{output}");
//! }
//! # });
//! ```
//!
//! ## Pumps
//!
//! A `Pump` is a wrapper around a common async programming (or rather multithreading) pattern - concurrent work is split into several tasks that communicate with each other using channels
//!
//! ```rust, ignore
//! let (sender0, mut receiver0) = mpsc::channel(100);
//! let (sender1, mut receiver1) = mpsc::channel(100);
//! let (sender2, mut receiver2) = mpsc::channel(100);
//!
//! tokio::spawn(async move {
//!     while let Some(x) = receiver0.recv().await {
//!         let output = work0(x).await;
//!         sender1.send(output).await.unwrap();
//!     }
//! });
//!
//! tokio::spawn(async move {
//!     while let Some(x) = receiver1.recv().await {
//!         let output = work1(x).await;
//!         sender2.send(output).await.unwrap();
//!     }
//! });
//!
//! // send data to input channel
//! send_input(sender0).await;
//!
//! while let Some(output) = receiver2.recv().await {
//!     println!("done with {}", output);
//! }
//! ```
//!
//! A 'Pump' is one step of such pipeline - a task and input/output channel. For example the `Map` Pump spawns a task, receives input via a `Receiver`, runs an async function, and sends its output to a `Sender`
//!
//! A [`Pipeline`] is a chain of `Pump`s. Each pump receives its input from the output channel of its predecessor
//!
//! ### Creation
//!
//! ```rust
//! use pumps::Pipeline;
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! // from channel
//! let (sender, receiver) = tokio::sync::mpsc::channel::<u32>(100);
//! let (mut output_receiver, join_handle) = Pipeline::from(receiver).build();
//!
//! // from a stream
//! let stream = futures::stream::iter(vec![1, 2, 3]);
//! let (mut output_receiver, join_handle) = Pipeline::from_stream(stream).build();
//!
//! // from an IntoIterator
//! let iter = vec![1, 2, 3];
//! let (mut output_receiver, join_handle) = Pipeline::from_iter(iter).build();
//! # });
//! ```
//!
//! The `.build()` method returns a touple of a `tokio::sync::mpsc::Receiver` and a join handle to the internally spawned tasks
//!
//! ### Concurrency control
//!
//! Each Pump operation receives a [`Concurrency`] struct that defines the concurrency characteristics of the operation.
//! - serial execution - `Concurrency::serial()`
//! - concurrent execution - `Concurrency::concurrent_ordered(n)`, `Concurrency::concurrent_unordered(n)`
//!
//! #### Backpressure
//!
//! Backpressure defines the amount of unconsumed data that can accumulate in memory. Without backpressure an eger operation will keep processing data and storing it in memory. A slow downstream consumer will result with unbounded memory usage. On the other hand, if we limit the in-memory buffering to 1, slow downstream consumer will often hang processing and introduce inefficiencies to the pipeline.
//! By default, the output channels of the various supplied pumps are with buffer size 1. Adding backpressure before potentially slow operations can improve processing efficiency.
//!
//! The `.backpressure(n)` operation limits the output channel of a `Pump` allowing it to stop processing data until the output channel have been consumed.
//! The `.backpressure_with_relief_valve(n)` operation is similar to `backpressure(n)` but instead of blocking the input channel it drops the oldest inputs.

mod concurrency;
mod concurrency_base;
mod pipeline;
mod pumps;

#[cfg(test)]
mod test_utils;

pub use concurrency::Concurrency;
pub use pipeline::Pipeline;
pub use pumps::flatten::FlattenConcurrency;
pub use pumps::pump::Pump;
