use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    future::FusedFuture,
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};

pub struct Concurrency {
    pub concurrency: usize,
    pub back_pressure: usize,
    pub preserve_order: bool,
}

impl Concurrency {
    pub fn concurrent(concurrency: usize) -> Self {
        Self {
            concurrency,
            back_pressure: 64,
            preserve_order: false,
        }
    }

    pub fn serial() -> Self {
        Self {
            concurrency: 1,
            back_pressure: 64,
            preserve_order: false,
        }
    }

    /// How many futurs can be stored in memory before a consumer takes them from the output channel
    /// (default = 0)
    pub fn backpressure(self, back_pressure: usize) -> Self {
        Self {
            concurrency: self.concurrency,
            back_pressure: back_pressure,
            preserve_order: self.preserve_order,
        }
    }

    pub fn preserve_order(self) -> Self {
        Self {
            concurrency: self.concurrency,
            back_pressure: self.back_pressure,
            preserve_order: true,
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

    pub(crate) fn select_next_some(&mut self) -> SelectNextSome<'_, T> {
        match self {
            Self::Ordered(futures) => SelectNextSome::Ordered(futures.select_next_some()),
            Self::Unordered(futures) => SelectNextSome::Unordered(futures.select_next_some()),
        }
    }
}

pub(crate) enum SelectNextSome<'a, T>
where
    T: Future,
{
    Ordered(futures::stream::SelectNextSome<'a, FuturesOrdered<T>>),
    Unordered(futures::stream::SelectNextSome<'a, FuturesUnordered<T>>),
}

impl<T> Future for SelectNextSome<'_, T>
where
    T: Future,
{
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut *self {
            Self::Ordered(select_next_some) => Pin::new(select_next_some).poll(cx),
            Self::Unordered(select_next_some) => Pin::new(select_next_some).poll(cx),
        }
    }
}

impl<T> FusedFuture for SelectNextSome<'_, T>
where
    T: Future,
{
    fn is_terminated(&self) -> bool {
        match self {
            Self::Ordered(select_next_some) => select_next_some.is_terminated(),
            Self::Unordered(select_next_some) => select_next_some.is_terminated(),
        }
    }
}
