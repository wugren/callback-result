# callback-result
Assists in converting the callback function's method of obtaining results into the await method

```rust
let waiter = Arc::new(CallbackWaiter::new());
let callback_id = 1;
let result_future = waiter.create_result_future(callback_id);
let tmp_waiter = waiter.clone();
async_std::task::spawn(async move {
    async_std::task::sleep(Duration::from_millis(1000)).await;
    tmp_waiter.set_result(callback_id, 1);
});
let ret = result_future.await.unwrap();
assert_eq!(ret, 1);
```

