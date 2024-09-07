use std::future::Future;

use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};

pub struct Concurrency {
    pub concurrency: usize,
    pub backpressure: usize,
    pub preserve_order: bool,
}

impl Concurrency {
    pub fn concurrent_unordered(concurrency: usize) -> Self {
        Self {
            concurrency,
            backpressure: concurrency,
            preserve_order: false,
        }
    }

    pub fn concurrent_ordered(concurrency: usize) -> Self {
        Self {
            concurrency,
            backpressure: concurrency,
            preserve_order: true,
        }
    }

    pub fn serial() -> Self {
        Self {
            concurrency: 1,
            backpressure: 1,
            preserve_order: false,
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
