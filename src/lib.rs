//! Eager streams for Rust. If a stream allows water to flow down the hill, a pump forces it up.
//!
//! This crate offers an alternative approach for rust async pipelining.
//!
//! Main features:
//!
//! - Designed for common async pipelining needs in heart
//! - Explicit concurrency, ordering, and backpressure control
//! - Eager - work is done before downstream methods consumes it
//! - builds on top of Rust async tools as tasks and channels.
//! - For now only supports the Tokio async runtime
//!
//! Example:
//!
//! ```rust, ignore
//! let (mut output_receiver, join_handle) = pumps::Pipeline::from_iter(urls)
//!     .map(get_json, Concurrency::concurrent_ordered(5).backpressure(100))
//!     .map(download_heavy_resource, Concurrency::serial())
//!     .filter_map(run_algorithm, Concurrency::concurrent_unordered(5).backpressure(10))
//!     .map(save_to_db, Concurrency::concurrent_unordered(100))
//!     .build();
//!
//! while let Some(output) = output_receiver.recv().await {
//!     println!("{output}");
//! }
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
//! A `Pipeline` is a chain of `Pump`s. Each pump receives its input from the output channel of its predecessor
//!
//! ### Creation
//!
//! ```rust,ignore
//! // from channel
//! let (mut output_receiver, join_handle) = Pipeline::from(tokio_channel);
//!
//! // from a stream
//! let (mut output_receiver, join_handle) = Pipeline::from_stream(stream);
//!
//! // create an iterator
//! let (mut output_receiver, join_handle) = Pipeline::from_iter(iter);
//! ```
//!
//! The `.build()` method returns a touple of a `tokio::sync::mpsc::Receiver` and a join handle to the internally spawned tasks
//!
//! ### Concurrency control
//!
//! Each Pump operation receives a `Concurrency` struct that defines the concurrency characteristics of the operation.
//! - serial execution - `Concurrency::serial()`
//! - concurrent execution - `Concurrency::concurrent_ordered(n)`, `Concurrency::concurrent_unordered(n)`
//!
//! #### Backpressure
//!
//! Backpressure defines the amount of unconsumed data can accumulate in memory. Without back pressure an eger operation will keep processing data and storing it in memory. A slow downstream consumer will result with unbounded memory usage.
//!
//! The `.backpressure(n)` definition limits the output channel of a `Pump` allowing it to stop processing data until the output channel have been consumed.
//!
//! The default backpressure is equal to the concurrency number

pub mod concurency;
pub mod filter_map;
pub mod map;
pub mod pumps;
pub mod test_utils;
