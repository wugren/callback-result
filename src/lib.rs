use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Mutex};
use notify_future::{Notify};

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum WaiterError {
    AlreadyExist,
    Timeout,
    NoWaiter,
}

impl Display for WaiterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WaiterError::AlreadyExist => write!(f, "AlreadyExist"),
            WaiterError::Timeout => write!(f, "Timeout"),
            WaiterError::NoWaiter => write!(f, "NoWaiter"),
        }
    }
}

impl std::error::Error for WaiterError {

}
pub type WaiterResult<T> = Result<T, WaiterError>;

pub struct ResultFuture<'a, R> {
    future: Pin<Box<dyn Future<Output = Result<R, WaiterError>> + 'a + Send>>,
}

impl <'a, R> ResultFuture<'a, R> {
    pub fn new(future: Pin<Box<dyn Future<Output = Result<R, WaiterError>> + 'a + Send>>) -> Self {
        Self {
            future,
        }
    }
}

impl <'a, R> Future for ResultFuture<'a, R> {
    type Output = Result<R, WaiterError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

struct CallbackWaiterState<K, R> {
    result_notifies: HashMap<K, Option<Notify<R>>>,
    result_cache: HashMap<K, Vec<R>>,
}
pub struct CallbackWaiter<K, R> {
    state: Mutex<CallbackWaiterState<K, R>>,
}

impl <K: Hash + Eq + Clone + 'static + Send, R: 'static + Send> CallbackWaiter<K, R> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(CallbackWaiterState {
                result_notifies: HashMap::new(),
                result_cache: HashMap::new(),
            })
        }
    }

    pub fn create_result_future(&self, callback_id: K) -> WaiterResult<ResultFuture<R>> {
        let waiter = {
            let mut state = self.state.lock().unwrap();
            let notifies = state.result_notifies.get(&callback_id);
            if let Some(notifies) = notifies {
                if let Some(notifies) = notifies {
                    if!notifies.is_canceled() {
                        return Err(WaiterError::AlreadyExist);
                    }
                }
            }
            if let Some(result) = state.result_cache.get_mut(&callback_id) {
                if result.len() > 0 {
                    let ret = result.remove(0);
                    return Ok(ResultFuture::new(Box::pin(async move {
                        Ok(ret)
                    })));
                }
            }

            let (notify, waiter) = Notify::new();
            state.result_notifies.insert(callback_id.clone(), Some(notify));
            waiter
        };

        Ok(ResultFuture::new(Box::pin(async move {
            let ret = waiter.await;
            {
                let mut state = self.state.lock().unwrap();
                state.result_notifies.remove(&callback_id);
            }
            Ok(ret)
        })))
    }

    pub fn create_timeout_result_future(&self, callback_id: K, timeout: std::time::Duration) -> WaiterResult<ResultFuture<R>> {
        let waiter = {
            let mut state = self.state.lock().unwrap();
            let notifies = state.result_notifies.get(&callback_id);
            if let Some(notifies) = notifies {
                if let Some(notifies) = notifies {
                    if!notifies.is_canceled() {
                        return Err(WaiterError::AlreadyExist);
                    }
                }
            }

            if let Some(result) = state.result_cache.get_mut(&callback_id) {
                if result.len() > 0 {
                    let ret = result.remove(0);
                    return Ok(ResultFuture::new(Box::pin(async move {
                        Ok(ret)
                    })));
                }
            }

            let (notify, waiter) = Notify::new();
            state.result_notifies.insert(callback_id.clone(), Some(notify));
            waiter
        };
        Ok(ResultFuture::new(Box::pin(async move {
            let ret = async_std::future::timeout(timeout, waiter).await;
            {
                let mut state = self.state.lock().unwrap();
                state.result_notifies.remove(&callback_id);
            }
            match ret {
                Ok(ret) => Ok(ret),
                Err(_) => Err(WaiterError::Timeout)
            }
        })))
    }

    pub fn set_result(&self, callback_id: K, result: R) -> Result<(), WaiterError> {
        let mut state = self.state.lock().unwrap();
        if let Some(future) = state.result_notifies.get_mut(&callback_id) {
            if let Some(future) = future.take() {
                if !future.is_canceled() {
                    future.notify(result);
                    return Ok(());
                }
            }
        }
        Err(WaiterError::NoWaiter)
    }

    pub fn set_result_with_cache(&self, callback_id: K, result: R) {
        let mut state = self.state.lock().unwrap();
        if let Some(future) = state.result_notifies.get_mut(&callback_id) {
            if let Some(future) = future.take() {
                if !future.is_canceled() {
                    future.notify(result);
                    return;
                }
            }
        }
        if let Some(cache) = state.result_cache.get_mut(&callback_id) {
            cache.push(result);
        } else {
            state.result_cache.insert(callback_id, vec![result]);
        }
    }
}

struct SingleCallbackWaiterState<R> {
    result_notify: Option<Option<Notify<R>>>,
    result_cache: Vec<R>,
}

pub struct SingleCallbackWaiter<R> {
    state: Mutex<SingleCallbackWaiterState<R>>,
}

impl <R: 'static + Send> SingleCallbackWaiter<R> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(SingleCallbackWaiterState {
                result_notify: None,
                result_cache: Vec::new(),
            })
        }
    }

    pub fn create_result_future(&self) -> WaiterResult<ResultFuture<R>> {
        let waiter = {
            let mut state = self.state.lock().unwrap();
            if let Some(notify) = state.result_notify.as_ref() {
                if let Some(notify) = notify {
                    if !notify.is_canceled() {
                        return Err(WaiterError::AlreadyExist);
                    }
                }
            }

            if state.result_cache.len() > 0 {
                let ret = state.result_cache.remove(0);
                return Ok(ResultFuture::new(Box::pin(async move {
                    Ok(ret)
                })));
            }
            let (notify, waiter) = Notify::new();
            state.result_notify = Some(Some(notify));
            waiter
        };
        Ok(ResultFuture::new(Box::pin(async move {
            let ret = waiter.await;
            {
                let mut state = self.state.lock().unwrap();
                state.result_notify = None;
            }
            Ok(ret)
        })))
    }

    pub fn create_timeout_result_future(&self, timeout: std::time::Duration) -> WaiterResult<ResultFuture<R>> {
        let waiter = {
            let mut state = self.state.lock().unwrap();
            if let Some(notify) = state.result_notify.as_ref() {
                if let Some(notify) = notify {
                    if !notify.is_canceled() {
                        return Err(WaiterError::AlreadyExist);
                    }
                }
            }

            if state.result_cache.len() > 0 {
                let ret = state.result_cache.remove(0);
                return Ok(ResultFuture::new(Box::pin(async move {
                    Ok(ret)
                })));
            }

            let (notify, waiter) = Notify::new();
            state.result_notify = Some(Some(notify));
            waiter
        };
        Ok(ResultFuture::new(Box::pin(async move {
            let ret = async_std::future::timeout(timeout, waiter).await;
            {
                let mut state = self.state.lock().unwrap();
                state.result_notify = None;
            }
            match ret {
                Ok(ret) => Ok(ret),
                Err(_) => {
                    Err(WaiterError::Timeout)
                }
            }
        })))
    }

    pub fn set_result(&self, result: R) -> Result<(), WaiterError> {
        let mut state = self.state.lock().unwrap();
        if let Some(future) = state.result_notify.as_mut() {
            if let Some(future) = future.take() {
                if !future.is_canceled() {
                    future.notify(result);
                    return Ok(());
                }
            }
        }
        Err(WaiterError::NoWaiter)
    }

    pub fn set_result_with_cache(&self, result: R) {
        let mut state = self.state.lock().unwrap();
        if let Some(future) = state.result_notify.as_mut() {
            if let Some(future) = future.take() {
                if !future.is_canceled() {
                    future.notify(result);
                    return;
                }
            }
        }
        state.result_cache.push(result);
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
            let result_future = waiter.create_result_future(callback_id).unwrap();
            assert!(waiter.create_result_future(callback_id).is_err());
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
            let result_future = waiter.create_result_future(callback_id).unwrap();
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
            let result_future = waiter.create_timeout_result_future(callback_id, Duration::from_secs(2)).unwrap();
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
            let result_future = waiter.create_timeout_result_future(callback_id, Duration::from_secs(2)).unwrap();
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                async_std::task::sleep(Duration::from_secs(3)).await;
                let ret = tmp.set_result(callback_id, 1);
                assert!(ret.is_err());
            });
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
            let result_future = waiter.create_timeout_result_future(callback_id, Duration::from_secs(2)).unwrap();
            assert!(waiter.create_timeout_result_future(callback_id, Duration::from_secs(2)).is_err());
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
            let result_future = waiter.create_result_future().unwrap();
            assert!(waiter.create_result_future().is_err());
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
            let result_future = waiter.create_result_future().unwrap();
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
            let result_future = waiter.create_timeout_result_future(Duration::from_secs(2)).unwrap();
            assert!(waiter.create_timeout_result_future(Duration::from_secs(2)).is_err());
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
            let result_future = waiter.create_timeout_result_future(Duration::from_secs(2)).unwrap();
            let tmp = waiter.clone();
            async_std::task::spawn(async move {
                async_std::task::sleep(Duration::from_secs(3)).await;
                let ret = tmp.set_result(1);
                assert!(ret.is_err());
            });
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
            let result_future = waiter.create_timeout_result_future(Duration::from_secs(2)).unwrap();
            match result_future.await {
                Ok(_) => {}
                Err(e) => {
                    assert_eq!(e, WaiterError::Timeout);
                }
            }
        });
    }
}
