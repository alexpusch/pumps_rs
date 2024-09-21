use std::future::Future;

use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};

/// Controls concurrency characteristics of a Pump operation
///
/// Example:
///
/// ```rust
/// use pumps::Concurrency;
///
/// // define a concurrent operation with 10 concurrent futures with backpressure of 100
/// let concurrency = Concurrency::concurrent_ordered(10).backpressure(100);
/// ```
pub struct Concurrency {
    /// How many futures can be run concurrently in the configured operation
    pub concurrency: usize,
    /// How many future results can be stored in memory before a consumer receives them from the output channel.
    /// In other words, this is the size of the output channel.
    /// When the output channel is full, the operation will stop processing additioanl data
    /// Defaults to the concurrency number
    pub backpressure: usize,
    /// whether to preserve the order of the input stream
    pub preserve_order: bool,
}

impl Concurrency {
    /// Defines an unordered concurrency with given number of concurrent futures executions
    pub fn concurrent_unordered(concurrency: usize) -> Self {
        Self {
            concurrency,
            backpressure: concurrency,
            preserve_order: false,
        }
    }

    /// Defines an ordered concurrency with given number of concurrent futures executions
    pub fn concurrent_ordered(concurrency: usize) -> Self {
        Self {
            concurrency,
            backpressure: concurrency,
            preserve_order: true,
        }
    }

    /// Defines a serial concurrency with only one future execution at a time
    pub fn serial() -> Self {
        Self {
            concurrency: 1,
            backpressure: 1,
            preserve_order: true,
        }
    }

    /// How many futures can be stored in memory before a consumer takes them from the output channel
    /// (default = concurrency)
    pub fn backpressure(self, backpressure: usize) -> Self {
        Self {
            backpressure,
            ..self
        }
    }
}

impl Default for Concurrency {
    fn default() -> Self {
        Self::serial()
    }
}

/// A wrapper around `FuturesOrdered` and `FuturesUnordered` that allows for a unified interface
/// and conigureable order
#[derive(Debug)]
pub(crate) enum FuturesContainer<T>
where
    T: Future,
{
    Ordered(FuturesOrdered<T>),
    Unordered(FuturesUnordered<T>),
}

impl<T> FuturesContainer<T>
where
    T: Future,
{
    pub(crate) fn new(preserve_order: bool) -> Self {
        match preserve_order {
            true => Self::Ordered(FuturesOrdered::new()),
            false => Self::Unordered(FuturesUnordered::new()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Ordered(futures) => futures.len(),
            Self::Unordered(futures) => futures.len(),
        }
    }

    pub(crate) fn push_back(&mut self, future: T) {
        match self {
            Self::Ordered(futures) => futures.push_back(future),
            Self::Unordered(futures) => futures.push(future),
        }
    }

    pub(crate) async fn next(&mut self) -> Option<T::Output> {
        match self {
            Self::Ordered(futures) => futures.next().await,
            Self::Unordered(futures) => futures.next().await,
        }
    }
}
