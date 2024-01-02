use std::{
    sync::{Arc, Mutex},
    thread,
};

use futures::channel::oneshot;

use super::{disk_manager::DiskManager, page::PageId};

/// @brief Represents a Write or Read request for the DiskManager to execute.
struct DiskRequest {
    /// Flag indicating whether the request is a write or a read.
    is_write: bool,

    ///  Pointer to the start of the memory location where a page is either:
    ///   1. being read into from disk (on a read).
    ///   2. being written out to disk (on a write).
    data: *mut char, // Consider using a safer type in Rust

    /// ID of the page being read from / written to disk.
    page_id: PageId,

    /// Callback used to signal to the request issuer when the request has been
    /// completed.
    callback: oneshot::Sender<bool>,
}

/// @brief The DiskScheduler schedules disk read and write operations.
///
/// A request is scheduled by calling DiskScheduler::Schedule() with an
/// appropriate DiskRequest object. The scheduler maintains a background worker
/// thread that processes the scheduled requests using the disk manager. The
/// background thread is created in the DiskScheduler constructor and joined in
/// its destructor.
struct DiskScheduler {
    /// Pointer to the disk manager.
    disk_manager: Arc<Mutex<DiskManager>>,

    /// A shared queue to concurrently schedule and process requests. When the
    /// DiskScheduler's destructor is called, `None` is put into the queue
    /// to signal to the background thread to stop execution.
    request_queue: crossbeam::channel::Sender<Option<DiskRequest>>,

    /// The background thread responsible for issuing scheduled requests to the
    /// disk manager.
    background_thread: Option<thread::JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<Mutex<DiskManager>>) -> Self {
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Schedules a request for the DiskManager to execute.
    ///
    /// @param r The request to be scheduled.
    pub fn schedule(&self, r: DiskRequest) {
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Background worker thread function that processes scheduled
    /// requests.
    ///
    /// The background thread needs to process requests while the DiskScheduler
    /// exists, i.e., this function should not return until ~DiskScheduler()
    /// is called. At that point you need to make sure that the function does
    /// return.
    fn start_worker_thread(&mut self) {
        unimplemented!()
    }

    /// @brief Create a Promise object. If you want to implement your own
    /// version of promise, you can change this function so that our test
    /// cases can use your promise implementation.
    ///
    /// @return std::promise<bool>
    fn create_promise() -> oneshot::Sender<bool> {
        unimplemented!()
    }
}
