use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Mutex;
use notify_future::NotifyFuture;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum WaiterError {
    AlreadyExist,
    Timeout,
}

pub struct CallbackWaiter<K, R> {
    futures: Mutex<HashMap<K, NotifyFuture<R>>>
}

impl <K: Hash + Eq + Clone, R: Clone> CallbackWaiter<K, R> {
    pub fn new() -> Self {
        Self {
            futures: Mutex::new(HashMap::new())
        }
    }

    pub fn create_result_future<'a, 'b: 'a>(&'b self, callback_id: K) -> impl Future<Output = Result<R, WaiterError>> + 'a {
        async move {
            let future = {
                let mut futures = self.futures.lock().unwrap();
                if futures.contains_key(&callback_id) {
                    return Err(WaiterError::AlreadyExist);
                }
                let future = NotifyFuture::new();
                futures.insert(callback_id.clone(), future.clone());
                future
            };
            let ret = future.await;
            {
                let mut futures = self.futures.lock().unwrap();
                futures.remove(&callback_id);
            }
            Ok(ret)
        }
    }

    pub fn create_timeout_result_future<'a, 'b: 'a>(&'b self, callback_id: K, timeout: std::time::Duration) -> impl Future<Output = Result<R, WaiterError>> + 'a {
        async move {
            let future = {
                let mut futures = self.futures.lock().unwrap();
                if futures.contains_key(&callback_id) {
                    return Err(WaiterError::AlreadyExist);
                }

                let future = NotifyFuture::new();
                futures.insert(callback_id.clone(), future.clone());
                future
            };
            let ret = async_std::future::timeout(timeout, future).await;
            {
                let mut futures = self.futures.lock().unwrap();
                futures.remove(&callback_id);
            }
            match ret {
                Ok(ret) => Ok(ret),
                Err(_) => Err(WaiterError::Timeout)
            }
        }
    }

    pub fn set_result(&self, callback_id: K, result: R) {
        let futures = self.futures.lock().unwrap();
        if let Some(future) = futures.get(&callback_id) {
            future.set_complete(result);
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    #[test]
    fn test_waiter() {
        use async_std::task;
        use std::time::Duration;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(CallbackWaiter::new());
            let callback_id = 1;
            let result_future = waiter.create_result_future(callback_id);
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                async_std::task::sleep(Duration::from_millis(1000)).await;
                tmp.set_result(callback_id, 1);
            });
            let ret = result_future.await.unwrap();
            assert_eq!(ret, 1);
        });
    }

    #[test]
    fn test_waiter_timout() {
        use async_std::task;
        use std::time::Duration;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(CallbackWaiter::new());
            let callback_id = 1;
            let result_future = waiter.create_timeout_result_future(callback_id, Duration::from_secs(2));
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                async_std::task::sleep(Duration::from_millis(1000)).await;
                tmp.set_result(callback_id, 1);
            });
            let ret = result_future.await.unwrap();
            assert_eq!(ret, 1);
        });
    }

    #[test]
    fn test_waiter_timout2() {
        use async_std::task;
        use std::time::Duration;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(CallbackWaiter::new());
            let callback_id = 1;
            let result_future = waiter.create_timeout_result_future(callback_id, Duration::from_secs(2));
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                tmp.set_result(callback_id, 1);
            }).await;
            match result_future.await {
                Ok(_) => {}
                Err(e) => {
                    assert_eq!(e, WaiterError::Timeout);
                }
            }
        });
    }
}
