#[macro_export]
macro_rules! concurrency_base {
    (
        input_receiver = $input_receiver:expr;
        concurrency = $concurrency_var_name:expr;

        on_input($input_var_name:ident, $in_progress_var_name:ident) => $input:block,
        on_progress($output_var_name:ident, $output_sender_var_name:ident) => $output:block) => {
           { let ($output_sender_var_name, output_receiver) =
                tokio::sync::mpsc::channel($concurrency_var_name.backpressure);

            let join_handle = tokio::spawn(async move {
                let mut $in_progress_var_name = $crate::concurrency::FuturesContainer::new($concurrency_var_name.preserve_order);

                loop {
                    let in_progress_len = $in_progress_var_name.len();
                    tokio::select! {
                        biased;

                        Some($input_var_name) = $input_receiver.recv(), if in_progress_len < $concurrency_var_name.concurrency => {
                            $input
                        },
                        Some($output_var_name) = $in_progress_var_name.next(), if in_progress_len > 0 => {
                            $output
                        },
                        else => break
                    }
                }
            });

            (output_receiver, join_handle)}
    };
}

#[cfg(test)]
mod test {
    use tokio::sync::mpsc;

    use crate::{
        concurrency::Concurrency,
        test_utils::{FutureTimings, TestValue},
    };

    #[tokio::test]
    async fn serial() {
        let concurrency = Concurrency::serial();
        let (input_sender, mut input_receiver) = mpsc::channel(100);

        let timings = FutureTimings::new();

        let map_fn = timings.get_tracked_fn(|value| value.id);

        let (mut output_receiver, _join_handle) = concurrency_base! {
            input_receiver = input_receiver;
            concurrency = concurrency;

            on_input(input, in_progress) => {
                let f = map_fn(input);
                in_progress.push_back(f);
            },
            on_progress(output, output_sender) => {
                if let Err(_e) = output_sender.send(output).await {
                    break;
                }
            }
        };

        // values are sent with decreasing duration, but will be executed in order
        input_sender.send(TestValue::new(1, 30)).await.unwrap();
        input_sender.send(TestValue::new(2, 20)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(3));

        assert!(timings.run_after(3, 2).await);
        assert!(timings.run_after(2, 1).await);

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);
    }

    #[tokio::test]
    async fn concurrency_2_unordered() {
        let concurrency = Concurrency::concurrent_unordered(2);
        let (input_sender, mut input_receiver) = mpsc::channel(100);

        let timings = FutureTimings::new();

        let map_fn = timings.get_tracked_fn(|value| value.id);

        let (mut output_receiver, _join_handle) = concurrency_base! {
            input_receiver = input_receiver;
            concurrency = concurrency;

            on_input(input, in_progress) => {
                let f = map_fn(input);
                in_progress.push_back(f);
            },
            on_progress(output, output_sender) => {
                if let Err(_e) = output_sender.send(output).await {
                    break;
                }
            }
        };

        // (2) finishes first, (1) and (3) are executed concurrently
        input_sender.send(TestValue::new(1, 20)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 15)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(3));

        assert!(timings.run_in_parallel(1, 2).await);
        assert!(timings.run_in_parallel(1, 3).await);
        assert!(timings.run_after(3, 2).await);

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);
    }

    #[tokio::test]
    async fn concurrency_2_ordered() {
        let concurrency = Concurrency::concurrent_ordered(2);
        let (input_sender, mut input_receiver) = mpsc::channel(100);

        let timings = FutureTimings::new();

        let map_fn = timings.get_tracked_fn(|value| value.id);

        let (mut output_receiver, _join_handle) = concurrency_base! {
            input_receiver = input_receiver;
            concurrency = concurrency;

            on_input(input, in_progress) => {
                let f = map_fn(input);
                in_progress.push_back(f);
            },
            on_progress(output, output_sender) => {
                if let Err(_e) = output_sender.send(output).await {
                    break;
                }
            }
        };

        // (2) finishes first, but (2) and (3) are executed concurrently to keep order
        input_sender.send(TestValue::new(1, 20)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 15)).await.unwrap();

        assert_eq!(output_receiver.recv().await, Some(1));
        assert_eq!(output_receiver.recv().await, Some(2));
        assert_eq!(output_receiver.recv().await, Some(3));

        assert!(timings.run_in_parallel(1, 2).await);
        assert!(timings.run_after(3, 2).await);

        drop(input_sender);

        assert_eq!(output_receiver.recv().await, None);
    }

    #[tokio::test]
    async fn concurrency_2_ordered_stops_without_consumer() {
        let concurrency = Concurrency::concurrent_ordered(2);
        let (input_sender, mut input_receiver) = mpsc::channel(100);

        let timings = FutureTimings::new();

        let map_fn = timings.get_tracked_fn(|value| value.id);

        let (_output_receiver, _join_handle) = concurrency_base! {
            input_receiver = input_receiver;
            concurrency = concurrency;

            on_input(input, in_progress) => {
                let f = map_fn(input);
                in_progress.push_back(f);
            },
            on_progress(output, output_sender) => {
                println!("on progress inner");
                if let Err(_e) = output_sender.send(output).await {
                    break;
                }
            }
        };

        input_sender.send(TestValue::new(1, 10)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();
        input_sender.send(TestValue::new(4, 10)).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // 4 did not run, as there is no more space in the output channel
        assert!(timings.is_completed(1).await);
        assert!(timings.is_completed(2).await);
        assert!(timings.is_completed(3).await);
        assert!(!timings.is_completed(4).await);
    }

    #[tokio::test]
    async fn concurrency_2_ordered_backpressure_3() {
        let concurrency = Concurrency::concurrent_ordered(2).backpressure(3);
        let (input_sender, mut input_receiver) = mpsc::channel(100);

        let timings = FutureTimings::new();

        let map_fn = timings.get_tracked_fn(|value| value.id);

        let (_output_receiver, _join_handle) = concurrency_base! {
            input_receiver = input_receiver;
            concurrency = concurrency;

            on_input(input, in_progress) => {
                let f = map_fn(input);
                in_progress.push_back(f);
            },
            on_progress(output, output_sender) => {
                println!("on progress inner");
                if let Err(_e) = output_sender.send(output).await {
                    break;
                }
            }
        };

        input_sender.send(TestValue::new(1, 10)).await.unwrap();
        input_sender.send(TestValue::new(2, 10)).await.unwrap();
        input_sender.send(TestValue::new(3, 10)).await.unwrap();
        input_sender.send(TestValue::new(4, 10)).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        timings.debug().await;

        // 4 will run, as there is space in the output channel thanks to backpressure
        assert!(timings.is_completed(1).await);
        assert!(timings.is_completed(2).await);
        assert!(timings.is_completed(3).await);
        assert!(timings.is_completed(4).await);
    }
}
