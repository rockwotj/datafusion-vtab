//! A Rust future that JavaScript drives by calling `poll`.
//!
//! `wasm_bindgen_futures::future_to_promise` polls futures from a microtask.
//! If the WASM instance traps during that poll (heap cap reached, panic), the
//! exception escapes into the microtask queue, where the caller cannot catch
//! it. Polling from JavaScript keeps every entry into Rust inside a call that
//! the TypeScript wrapper guards with try/catch. JavaScript promises are still
//! awaited from Rust with `wasm_bindgen_futures::JsFuture`.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use wasm_bindgen::prelude::*;

pub type BoxFuture = Pin<Box<dyn Future<Output = Result<JsValue, JsValue>>>>;

struct WakeSlot {
    callback: RefCell<Option<js_sys::Function>>,
    woken: Cell<bool>,
}

// The engine runs on one thread. `Waker` requires `Send + Sync`.
unsafe impl Send for WakeSlot {}
unsafe impl Sync for WakeSlot {}

impl Wake for WakeSlot {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.set(true);
        let callback = self.callback.borrow_mut().take();
        if let Some(callback) = callback {
            let _ = callback.call0(&JsValue::UNDEFINED);
        }
    }
}

#[wasm_bindgen]
pub struct Task {
    future: Option<BoxFuture>,
    result: Option<Result<JsValue, JsValue>>,
    slot: Arc<WakeSlot>,
}

impl Task {
    pub fn new(future: impl Future<Output = Result<JsValue, JsValue>> + 'static) -> Task {
        Task {
            future: Some(Box::pin(future)),
            result: None,
            slot: Arc::new(WakeSlot {
                callback: RefCell::new(None),
                woken: Cell::new(false),
            }),
        }
    }
}

#[wasm_bindgen]
impl Task {
    /// Polls the future once. Returns true when it has finished. Otherwise
    /// `on_wake` is called once the future can make progress, which may
    /// happen before `poll` returns.
    pub fn poll(&mut self, on_wake: js_sys::Function) -> bool {
        let Some(future) = self.future.as_mut() else {
            return true;
        };
        self.slot.woken.set(false);
        *self.slot.callback.borrow_mut() = Some(on_wake);
        let waker = Waker::from(Arc::clone(&self.slot));
        let mut cx = Context::from_waker(&waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(result) => {
                self.future = None;
                self.result = Some(result);
                self.slot.callback.borrow_mut().take();
                true
            }
            Poll::Pending => false,
        }
    }

    /// Returns the value of the finished future, or throws its error.
    #[wasm_bindgen(js_name = takeResult)]
    pub fn take_result(&mut self) -> Result<JsValue, JsValue> {
        self.result
            .take()
            .unwrap_or_else(|| Err(JsValue::from_str("task has not finished")))
    }

    /// Drops the future, which releases scans and memory it holds.
    pub fn cancel(&mut self) {
        self.future = None;
        self.slot.callback.borrow_mut().take();
    }
}
