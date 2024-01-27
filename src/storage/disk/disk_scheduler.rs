use std::thread;

use tokio::sync::oneshot;

use crate::storage::disk::DiskManager;
use crate::storage::page::Page;

/// @brief Represents a Write or Read request for the DiskManager to execute.
pub enum DiskRequest {
    Read {
        /// The page being read from disk.
        page: Page,
        /// Callback used to signal to the request issuer when the request has
        /// been completed.
        callback: oneshot::Sender<()>,
    },
    Write {
        /// The page being written out ot disk.
        page: Page,
        /// Callback used to signal to the request issuer when the request has
        /// been completed.
        callback: oneshot::Sender<()>,
    },
}

/// @brief The DiskScheduler schedules disk read and write operations.
///
/// A request is scheduled by calling DiskScheduler::Schedule() with an
/// appropriate DiskRequest object. The scheduler maintains a background worker
/// thread that processes the scheduled requests using the disk manager. The
/// background thread is created in the DiskScheduler constructor and joined in
/// its destructor.
pub struct DiskScheduler {
    /// A shared queue to concurrently schedule and process requests. When the
    /// DiskScheduler's destructor is called, `None` is put into the queue
    /// to signal to the background thread to stop execution.
    request_queue: std::sync::mpsc::Sender<Option<DiskRequest>>,

    /// The background thread responsible for issuing scheduled requests to the
    /// disk manager.
    background_thread: Option<thread::JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: DiskManager) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        Self {
            request_queue: tx,
            background_thread: Some(thread::spawn(move || {
                Self::start_worker_thread(rx, disk_manager)
            })),
        }
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Schedules a request for the DiskManager to execute.
    ///
    /// @param r The request to be scheduled.
    pub fn schedule(&self, r: DiskRequest) {
        self.request_queue.send(Some(r)).unwrap();
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
    fn start_worker_thread(
        rx: std::sync::mpsc::Receiver<Option<DiskRequest>>,
        mut disk_manager: DiskManager,
    ) {
        while let Ok(r) = rx.recv() {
            match r {
                Some(DiskRequest::Read { page, callback }) => {
                    disk_manager.read_page(page.get_page_id().unwrap(), &mut *page.get_mut_data());
                    callback.send(()).unwrap();
                }
                Some(DiskRequest::Write { page, callback }) => {
                    disk_manager.write_page(page.get_page_id().unwrap(), &*page.get_data());
                    callback.send(()).unwrap();
                }
                None => break,
            }
        }
    }

    /// @brief Create a Promise object. If you want to implement your own
    /// version of promise, you can change this function so that our test
    /// cases can use your promise implementation.
    ///
    /// @return std::promise<bool>
    fn create_promise() -> oneshot::Sender<()> {
        unimplemented!()
    }
}

impl Drop for DiskScheduler {
    fn drop(&mut self) {
        // Put a `std::nullopt` in the queue to signal to exit the loop
        self.request_queue.send(None).unwrap();
        self.background_thread.take().unwrap().join().unwrap();
    }
}
