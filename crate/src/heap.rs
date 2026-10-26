//! Global allocator that caps the bytes Rust may hold on the WASM heap.
//!
//! When an allocation would exceed the cap, `alloc` returns null and Rust
//! aborts, which traps the WASM instance. The TypeScript wrapper catches the
//! trap, reads `heapLimitFailure()` to explain it, and discards the instance.
//! DataFusion's memory pool rejects most oversized queries long before this
//! cap is reached; the cap covers allocations the pool does not track.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Mutex;

use wasm_bindgen::prelude::*;

pub struct CappedAlloc;

// The engine runs on one thread, so plain cells are enough. `Sync` is needed
// because these are statics.
struct State {
    used: Cell<usize>,
    peak: Cell<usize>,
    limit: Cell<usize>,
    failed_request: Cell<usize>,
}

unsafe impl Sync for State {}

static STATE: State = State {
    used: Cell::new(0),
    peak: Cell::new(0),
    limit: Cell::new(usize::MAX),
    failed_request: Cell::new(0),
};

fn reserve(bytes: usize) -> bool {
    let used = STATE.used.get();
    match used.checked_add(bytes) {
        Some(next) if next <= STATE.limit.get() => {
            STATE.used.set(next);
            if next > STATE.peak.get() {
                STATE.peak.set(next);
            }
            true
        }
        _ => {
            STATE.failed_request.set(bytes);
            false
        }
    }
}

fn release(bytes: usize) {
    STATE.used.set(STATE.used.get().saturating_sub(bytes));
}

unsafe impl GlobalAlloc for CappedAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !reserve(layout.size()) {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.alloc(layout) };
        if ptr.is_null() {
            release(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if !reserve(layout.size()) {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if ptr.is_null() {
            release(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        release(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let old_size = layout.size();
        if new_size > old_size && !reserve(new_size - old_size) {
            return std::ptr::null_mut();
        }
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if new_ptr.is_null() {
            if new_size > old_size {
                release(new_size - old_size);
            }
        } else if new_size < old_size {
            release(old_size - new_size);
        }
        new_ptr
    }
}

static PANIC_MESSAGE: Mutex<Option<String>> = Mutex::new(None);

pub fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        if let Ok(mut slot) = PANIC_MESSAGE.lock() {
            *slot = Some(info.to_string());
        }
    }));
}

/// Sets the maximum number of bytes the WASM heap may hold.
#[wasm_bindgen(js_name = setHeapLimit)]
pub fn set_heap_limit(bytes: f64) {
    STATE.limit.set(if bytes.is_finite() && bytes >= 0.0 {
        bytes as usize
    } else {
        usize::MAX
    });
}

/// Returns `[used, peak, limit]` heap bytes. A limit of -1 means no limit.
#[wasm_bindgen(js_name = heapStats)]
pub fn heap_stats() -> Vec<f64> {
    let limit = STATE.limit.get();
    vec![
        STATE.used.get() as f64,
        STATE.peak.get() as f64,
        if limit == usize::MAX { -1.0 } else { limit as f64 },
    ]
}

/// Size of the allocation the heap cap rejected, or 0 if none was rejected.
#[wasm_bindgen(js_name = heapLimitFailure)]
pub fn heap_limit_failure() -> f64 {
    STATE.failed_request.get() as f64
}

/// Message of the last Rust panic, if any.
#[wasm_bindgen(js_name = takePanicMessage)]
pub fn take_panic_message() -> Option<String> {
    PANIC_MESSAGE.lock().ok().and_then(|mut slot| slot.take())
}
