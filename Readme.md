# pumps_rs

Eager streams for Rust. If a stream waits for the water to flow down the hill, a pump forces it up.

[Futures stream api](https://docs.rs/futures/latest/futures/stream/index.html#) is awesome, but has unfortunate issues

- Futures run in surprising and unintuitive order. Read about [Barbara battles buffered streams](https://rust-lang.github.io/wg-async/vision/submitted_stories/status_quo/barbara_battles_buffered_streams.html)
- Prone to surprising deadlocks. [Fixing the Next Thousand Deadlocks: Why Buffered Streams Are Broken and How To Make Them Safer](https://blog.polybdenum.com/2022/07/24/fixing-the-next-thousand-deadlocks-why-buffered-streams-are-broken-and-how-to-make-them-safer.html)
- [Rust Stream API visualized and exposed](https://github.com/alexpusch/rust-magic-patterns/blob/master/rust-stream-visualized/Readme.md)
- Fixing these issues will require added features to the language - [poll_progress](https://without.boats/blog/poll-progress/)

This crate offers an alternative approach for rust async pipelining.

Main features:

- Designed for common async pipelining needs in heart
- Explicit concurrency, ordering, and backpressure control
- Eager - work is done before downstream methods consumes it
- For now only supports the Tokio async runtime

Example:

```rust
let (mut output_receiver, join_handle) = pumps::Pipeline::from_iter(urls)
    .map(get_json, Concurrency::concurrent(5).preserve_order().backpressure(100))
    .map(download_heavy_resource, Concurrency::serial())
    .filter_map(run_algorithm, Concurrency::concurrent(5).backpressure(10))
    .map(save_to_db, Concurrency::concurrent(100))
    .build();

while let Some(output) = output_receiver.recv().await {
    println!("{output}");
}
```

## Pumps

A `Pump` is a wrapper around a common async programming (or rather multithreading) pattern - concurrent work is split into several tasks that communicate with each other using channels

```rust
let (sender0, mut receiver0) = mpsc::channel(100);
let (sender1, mut receiver1) = mpsc::channel(100);
let (sender2, mut receiver2) = mpsc::channel(100);

tokio::spawn(async move {
    while let Some(x) = receiver0.recv().await {
        let output = work0(x).await;
        sender1.send(output).await.unwrap();
    }
});

tokio::spawn(async move {
    while let Some(x) = receiver1.recv().await {
        let output = work1(x).await;
        sender2.send(output).await.unwrap();
    }
});

sender0.send(data).await.unwrap();

while let Some(output) = receiver2.recv().await {
    println!("done with {}", output);
}
```

A `Pipeline` is a chain of `Pump`s. Each `Pump` is a spawned task that receives data from the previous Pump via channel, runs some operation, and sends the output channel to the next `Pump`.

### Creation

```rust
// from channel
let (mut output_receiver, join_handle) = Pipeline::from(tokio_channel);

// from a stream
let (mut output_receiver, join_handle) = Pipeline::from_stream(stream);

// create an iterator
let (mut output_receiver, join_handle) = Pipeline::from_iter(iter);
```

The `.build()` method returns a touple of a `tokio::sync::mpsc::Receiver` and a join handle to the internally spawned tasks

### Concurrency control

Each Pump operation receives a `Concurrency` struct that defines the concurrency characteristics of the operation.

- Serial vs concurrency execution: `Concurrency::serial()`, `Concurrency::concurrent(n)`
- Unordered by default. Preserve order `Concurrency::concurrent(n).preserve_order()`

#### Backpressure

Backpressure defines the amount of unconsumed data can accumulate in memory. Without back pressure an eger operation will keep processing data and storing it in memory. A slow downstream consumer will result with unbounded memory usage.

The `.backpressure(n)` definition would limit the output channel of a `Pump` allowing it to stop processing data before output channel have been consumed.

The default backpressure is equal to the concurrency number

##
