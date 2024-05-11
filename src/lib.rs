use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Mutex;
use notify_future::NotifyFuture;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum WaiterError {
    AlreadyExist,
    Timeout,
    NoWaiter,
}

struct CallbackWaiterState<K, R> {
    futures: HashMap<K, NotifyFuture<R>>,
    result_cache: HashMap<K, Vec<R>>,
}
pub struct CallbackWaiter<K, R> {
    state: Mutex<CallbackWaiterState<K, R>>,
}

impl <K: Hash + Eq + Clone, R> CallbackWaiter<K, R> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(CallbackWaiterState {
                futures: HashMap::new(),
                result_cache: HashMap::new(),
            })
        }
    }

    pub fn create_result_future<'a, 'b: 'a>(&'b self, callback_id: K) -> impl Future<Output = Result<R, WaiterError>> + 'a {
        async move {
            let future = {
                let mut state = self.state.lock().unwrap();
                if state.futures.contains_key(&callback_id) {
                    return Err(WaiterError::AlreadyExist);
                }

                if let Some(result) = state.result_cache.get_mut(&callback_id) {
                    if result.len() > 0 {
                        return Ok(result.remove(0));
                    }
                }

                let future = NotifyFuture::new();
                state.futures.insert(callback_id.clone(), future.clone());
                future
            };
            let ret = future.await;
            {
                let mut state = self.state.lock().unwrap();
                state.futures.remove(&callback_id);
            }
            Ok(ret)
        }
    }

    pub fn create_timeout_result_future<'a, 'b: 'a>(&'b self, callback_id: K, timeout: std::time::Duration) -> impl Future<Output = Result<R, WaiterError>> + 'a {
        async move {
            let future = {
                let mut state = self.state.lock().unwrap();
                if state.futures.contains_key(&callback_id) {
                    return Err(WaiterError::AlreadyExist);
                }

                if let Some(result) = state.result_cache.get_mut(&callback_id) {
                    if result.len() > 0 {
                        return Ok(result.remove(0));
                    }
                }

                let future = NotifyFuture::new();
                state.futures.insert(callback_id.clone(), future.clone());
                future
            };
            let ret = async_std::future::timeout(timeout, future).await;
            {
                let mut state = self.state.lock().unwrap();
                state.futures.remove(&callback_id);
            }
            match ret {
                Ok(ret) => Ok(ret),
                Err(_) => Err(WaiterError::Timeout)
            }
        }
    }

    pub fn set_result(&self, callback_id: K, result: R) -> Result<(), WaiterError> {
        let state = self.state.lock().unwrap();
        if let Some(future) = state.futures.get(&callback_id) {
            future.set_complete(result);
            Ok(())
        } else {
            Err(WaiterError::NoWaiter)
        }
    }

    pub fn set_result_with_cache(&self, callback_id: K, result: R) {
        let mut state = self.state.lock().unwrap();
        if let Some(future) = state.futures.get(&callback_id) {
            future.set_complete(result);
        } else {
            if let Some(cache) = state.result_cache.get_mut(&callback_id) {
                cache.push(result);
            } else {
                state.result_cache.insert(callback_id, vec![result]);
            }
        }
    }
}

struct SingleCallbackWaiterState<R> {
    future: Option<NotifyFuture<R>>,
    result_cache: Vec<R>,
}

pub struct SingleCallbackWaiter<R> {
    state: Mutex<SingleCallbackWaiterState<R>>,
}

impl <R> SingleCallbackWaiter<R> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(SingleCallbackWaiterState {
                future: None,
                result_cache: Vec::new(),
            })
        }
    }

    pub fn create_result_future<'a, 'b: 'a>(&'b self) -> impl Future<Output = Result<R, WaiterError>> + 'a {
        async move {
            let future = {
                let mut state = self.state.lock().unwrap();
                if state.future.is_some() {
                    return Err(WaiterError::AlreadyExist);
                }

                if state.result_cache.len() > 0 {
                    return Ok(state.result_cache.remove(0));
                }
                let future = NotifyFuture::new();
                state.future = Some(future.clone());
                future
            };
            let ret = future.await;
            {
                let mut state = self.state.lock().unwrap();
                state.future = None;
            }
            Ok(ret)
        }
    }

    pub fn create_timeout_result_future<'a, 'b: 'a>(&'b self, timeout: std::time::Duration) -> impl Future<Output = Result<R, WaiterError>> + 'a {
        async move {
            let future = {
                let mut state = self.state.lock().unwrap();
                if state.future.is_some() {
                    return Err(WaiterError::AlreadyExist);
                }

                if state.result_cache.len() > 0 {
                    return Ok(state.result_cache.remove(0));
                }

                let future = NotifyFuture::new();
                state.future = Some(future.clone());
                future
            };
            let ret = async_std::future::timeout(timeout, future).await;
            {
                let mut state = self.state.lock().unwrap();
                state.future = None;
            }
            match ret {
                Ok(ret) => Ok(ret),
                Err(_) => Err(WaiterError::Timeout)
            }
        }
    }

    pub fn set_result(&self, result: R) -> Result<(), WaiterError> {
        let state = self.state.lock().unwrap();
        if let Some(future) = state.future.as_ref() {
            future.set_complete(result);
            Ok(())
        } else {
            Err(WaiterError::NoWaiter)
        }
    }

    pub fn set_result_with_cache(&self, result: R) {
        let mut state = self.state.lock().unwrap();
        if let Some(future) = state.future.as_ref() {
            future.set_complete(result);
        } else {
            state.result_cache.push(result);
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
                let ret = tmp.set_result(callback_id, 1);
                assert!(ret.is_ok());
            });
            let ret = result_future.await.unwrap();
            assert_eq!(ret, 1);
        });
    }

    #[test]
    fn test_waiter1() {
        use async_std::task;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(CallbackWaiter::new());
            let callback_id = 1;
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                tmp.set_result_with_cache(callback_id, 1);
            });
            let result_future = waiter.create_result_future(callback_id);
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
                let ret = tmp.set_result(callback_id, 1);
                assert!(ret.is_ok());
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
                async_std::task::sleep(Duration::from_millis(3000)).await;
                let ret = tmp.set_result(callback_id, 1);
                assert!(ret.is_err());
            }).await;
            match result_future.await {
                Ok(_) => {}
                Err(e) => {
                    assert_eq!(e, WaiterError::Timeout);
                }
            }
        });
    }

    #[test]
    fn test_waiter_timout3() {
        use async_std::task;
        use std::time::Duration;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(CallbackWaiter::new());
            let callback_id = 1;
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                let ret = tmp.set_result(callback_id, 1);
                assert!(ret.is_err());
            }).await;
            let result_future = waiter.create_timeout_result_future(callback_id, Duration::from_secs(2));
            match result_future.await {
                Ok(_) => {}
                Err(e) => {
                    assert_eq!(e, WaiterError::Timeout);
                }
            }
        });
    }

    #[test]
    fn test_signle_waiter() {
        use async_std::task;
        use std::time::Duration;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(SingleCallbackWaiter::new());
            let result_future = waiter.create_result_future();
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                async_std::task::sleep(Duration::from_millis(1000)).await;
                let ret = tmp.set_result(1);
                assert!(ret.is_ok());
            });
            let ret = result_future.await.unwrap();
            assert_eq!(ret, 1);
        });
    }

    #[test]
    fn test_single_waiter1() {
        use async_std::task;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(SingleCallbackWaiter::new());
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                tmp.set_result_with_cache(1);
            });
            let result_future = waiter.create_result_future();
            let ret = result_future.await.unwrap();
            assert_eq!(ret, 1);
        });
    }

    #[test]
    fn test_single_waiter_timout() {
        use async_std::task;
        use std::time::Duration;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(SingleCallbackWaiter::new());
            let result_future = waiter.create_timeout_result_future(Duration::from_secs(2));
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                async_std::task::sleep(Duration::from_millis(1000)).await;
                let ret = tmp.set_result(1);
                assert!(ret.is_ok());
            });
            let ret = result_future.await.unwrap();
            assert_eq!(ret, 1);
        });
    }

    #[test]
    fn test_single_waiter_timout2() {
        use async_std::task;
        use std::time::Duration;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(SingleCallbackWaiter::new());
            let result_future = waiter.create_timeout_result_future(Duration::from_secs(2));
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                async_std::task::sleep(Duration::from_millis(3000)).await;
                let ret = tmp.set_result(1);
                assert!(ret.is_err());
            }).await;
            match result_future.await {
                Ok(_) => {}
                Err(e) => {
                    assert_eq!(e, WaiterError::Timeout);
                }
            }
        });
    }

    #[test]
    fn test_single_waiter_timout3() {
        use async_std::task;
        use std::time::Duration;
        use super::*;
        task::block_on(async {
            let waiter = Arc::new(SingleCallbackWaiter::new());
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                let ret = tmp.set_result(1);
                assert!(ret.is_err());
            }).await;
            let result_future = waiter.create_timeout_result_future(Duration::from_secs(2));
            match result_future.await {
                Ok(_) => {}
                Err(e) => {
                    assert_eq!(e, WaiterError::Timeout);
                }
            }
        });
    }
}
