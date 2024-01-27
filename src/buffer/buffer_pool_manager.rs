use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use super::lru_k_replacer::LRUKReplacer;
use crate::common::config::{FrameId, PageId, BUSTUB_PAGE_SIZE, LRUK_REPLACER_K};
use crate::storage::disk::{DiskManager, DiskRequest, DiskScheduler};
use crate::storage::page::{BasicPageGuard, Page, ReadPageGuard, WritePageGuard};

/// BufferPoolManager reads disk pages to and from its internal buffer pool.
pub struct BufferPoolManager {
    /// Number of pages in the buffer pool.
    pool_size: usize,
    /// The next page id to be allocated
    next_page_id: AtomicUsize,

    /// Array of buffer pool pages.
    pages: Vec<Page>,
    /// Pointer to the disk scheduler.
    disk_scheduler: DiskScheduler,
    /// Pointer to the log manager. Please ignore this for P1.
    // log_manager: Option<Arc<LogManager>>,
    /// Page table for keeping track of buffer pool pages.
    page_table: Mutex<HashMap<PageId, FrameId>>,
    /// Replacer to find unpinned pages for replacement.
    replacer: LRUKReplacer,
    /// List of free frames that don't have any pages on them.
    free_list: Mutex<Vec<FrameId>>,
}

impl BufferPoolManager {
    /// @brief Creates a new BufferPoolManager.
    /// @param pool_size the size of the buffer pool
    /// @param disk_manager the disk manager
    /// @param replacer_k the LookBack constant k for the LRU-K replacer
    /// @param log_manager the log manager (for testing only: nullptr = disable
    /// logging). Please ignore this for P1.
    pub fn new(
        pool_size: usize,
        disk_manager: DiskManager,
        replacer_k: usize,
        // log_manager: Option<Arc<LogManager>>,
    ) -> BufferPoolManager {
        let mut free_list = Vec::with_capacity(pool_size);
        for i in (0..pool_size).rev() {
            free_list.push(i as FrameId);
        }
        Self {
            pool_size,
            next_page_id: AtomicUsize::new(0),
            pages: (0..pool_size).map(|_| Page::new()).collect(),
            disk_scheduler: DiskScheduler::new(disk_manager),
            // log_manager,
            page_table: Mutex::new(HashMap::new()),
            replacer: LRUKReplacer::new(replacer_k, LRUK_REPLACER_K),
            free_list: Mutex::new(free_list),
        }
    }

    /// @brief Return the size (number of frames) of the buffer pool.
    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    /// @brief Return the pointer to all the pages in the buffer pool.
    pub fn get_pages(&self) -> &Vec<Page> {
        &self.pages
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Create a new page in the buffer pool. Set page_id to the new
    /// page's id, or none if all frames are currently in use and not
    /// evictable (in another word, pinned).
    ///
    /// You should pick the replacement frame from either the free list or the
    /// replacer (always find from the free list first), and then call the
    /// AllocatePage() method to get a new page id. If the replacement frame has
    /// a dirty page, you should write it back to the disk first. You also
    /// need to reset the memory and metadata for the new page.
    ///
    /// Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id,
    /// false) so that the replacer wouldn't evict the frame before the
    /// buffer pool manager "Unpin"s it. Also, remember to record the access
    /// history of the frame in the replacer for the lru-k algorithm to work.
    ///
    /// @return none if no new pages could be created, otherwise pointer to
    /// new page
    pub fn new_page(&mut self) -> Option<Page> {
        let frame_id = if let Some(frame_id) = self.free_list.lock().unwrap().pop() {
            frame_id
        } else if let Some(frame_id) = self.replacer.evict() {
            let page = &self.pages[frame_id];
            if page.is_dirty() {
                let (tx, rx) = oneshot::channel();
                self.disk_scheduler.schedule(DiskRequest::Write {
                    page: page.clone(),
                    callback: tx,
                });
                rx.blocking_recv().unwrap();
            }
            self.page_table
                .lock()
                .unwrap()
                .remove(&page.get_page_id().unwrap());
            frame_id
        } else {
            return None;
        };

        let page_id = self.allocate_page();
        let page = &self.pages[frame_id];
        page.set_page_id(page_id);
        page.pin();
        self.page_table.lock().unwrap().insert(page_id, frame_id);
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        Some(page.clone())
    }

    /// TODO(P2): Add implementation
    ///
    /// @brief PageGuard wrapper for NewPage
    ///
    /// Functionality should be the same as NewPage, except that
    /// instead of returning a pointer to a page, you return a
    /// BasicPageGuard structure.
    ///
    /// @param[out] page_id, the id of the new page
    /// @return BasicPageGuard holding a new page
    pub fn new_page_guarded(&mut self, page_id: &mut Option<PageId>) -> Option<BasicPageGuard> {
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Fetch the requested page from the buffer pool. Return none if
    /// page_id needs to be fetched from the disk but all frames are
    /// currently in use and not evictable (in another word, pinned).
    ///
    /// First search for page_id in the buffer pool. If not found, pick a
    /// replacement frame from either the free list or the replacer (always
    /// find from the free list first), read the page from disk by scheduling a
    /// read DiskRequest with disk_scheduler_->Schedule(), and replace the
    /// old page in the frame. Similar to NewPage(), if the old page is dirty,
    /// you need to write it back to disk and update the metadata of the new
    /// page
    ///
    /// In addition, remember to disable eviction and record the access history
    /// of the frame like you did for NewPage().
    ///
    /// @param page_id id of page to be fetched
    /// @return nullptr if page_id cannot be fetched,
    /// otherwise pointer to the requested page
    pub fn fetch_page(&mut self, page_id: PageId) -> Option<Page> {
        if let Some(frame_id) = self.page_table.lock().unwrap().get(&page_id) {
            let page = &self.pages[*frame_id];
            page.pin();
            self.replacer.record_access(*frame_id);
            return Some(page.clone());
        }

        let frame_id = if let Some(frame_id) = self.free_list.lock().unwrap().pop() {
            frame_id
        } else if let Some(frame_id) = self.replacer.evict() {
            let page = &self.pages[frame_id];
            if page.is_dirty() {
                let (tx, rx) = oneshot::channel();
                self.disk_scheduler.schedule(DiskRequest::Write {
                    page: page.clone(),
                    callback: tx,
                });
                rx.blocking_recv().unwrap();
            }
            self.page_table
                .lock()
                .unwrap()
                .remove(&page.get_page_id().unwrap());
            frame_id
        } else {
            return None;
        };

        let page = &self.pages[frame_id];
        page.set_page_id(page_id);
        page.pin();
        let (tx, rx) = oneshot::channel();
        self.disk_scheduler.schedule(DiskRequest::Read {
            page: page.clone(),
            callback: tx,
        });
        rx.blocking_recv().unwrap();
        self.page_table.lock().unwrap().insert(page_id, frame_id);
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        Some(page.clone())
    }

    /// TODO(P2): Add implementation
    ///
    /// @brief PageGuard wrappers for FetchPage
    ///
    /// Functionality should be the same as FetchPage, except
    /// that, depending on the function called, a guard is returned.
    /// If FetchPageRead or FetchPageWrite is called, it is expected that
    /// the returned page already has a read or write latch held, respectively.
    ///
    /// @param page_id, the id of the page to fetch
    /// @return PageGuard holding the fetched page
    pub fn fetch_page_basic(&mut self, page_id: PageId) -> Option<BasicPageGuard> {
        unimplemented!()
    }
    pub fn fetch_page_read(&mut self, page_id: PageId) -> Option<ReadPageGuard> {
        unimplemented!()
    }
    pub fn fetch_page_write(&mut self, page_id: PageId) -> Option<WritePageGuard> {
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Unpin the target page from the buffer pool. If page_id is not in
    /// the buffer pool or its pin count is already 0, return false.
    ///
    /// Decrement the pin count of a page. If the pin count reaches 0, the frame
    /// should be evictable by the replacer. Also, set the dirty flag on the
    /// page to indicate if the page was modified.
    ///
    /// @param page_id id of page to be unpinned
    /// @param is_dirty true if the page should be marked as dirty, false
    /// otherwise @return false if the page is not in the page
    /// table or its pin count is <= 0 before this call, true otherwise
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> bool {
        if let Some(frame_id) = self.page_table.lock().unwrap().get(&page_id) {
            let page = &self.pages[*frame_id];
            if page.get_pin_count() <= 0 {
                return false;
            }
            page.set_dirty(is_dirty);
            page.unpin();
            if page.get_pin_count() == 0 {
                self.replacer.set_evictable(*frame_id, true);
            }
            true
        } else {
            false
        }
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Flush the target page to disk.
    ///
    /// Use the DiskManager::WritePage() method to flush a page to disk,
    /// REGARDLESS of the dirty flag. Unset the dirty flag of the page after
    /// flushing.
    ///
    /// @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
    /// @return false if the page could not be found in the page table, true
    /// otherwise
    pub fn flush_page(&mut self, page_id: PageId) -> bool {
        if let Some(frame_id) = self.page_table.lock().unwrap().get(&page_id) {
            let page = &self.pages[*frame_id];
            let (tx, rx) = oneshot::channel();
            self.disk_scheduler.schedule(DiskRequest::Write {
                page: page.clone(),
                callback: tx,
            });
            rx.blocking_recv().unwrap();
            true
        } else {
            false
        }
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Flush all the pages in the buffer pool to disk.
    pub fn flush_all_pages(&mut self) {
        for page in self.pages.iter() {
            if page.is_dirty() {
                let (tx, rx) = oneshot::channel();
                self.disk_scheduler.schedule(DiskRequest::Write {
                    page: page.clone(),
                    callback: tx,
                });
                rx.blocking_recv().unwrap();
            }
        }
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Delete a page from the buffer pool. If page_id is not in the
    /// buffer pool, do nothing and return true. If the page is pinned and
    /// cannot be deleted, return false immediately.
    ///
    /// After deleting the page from the page table, stop tracking the frame in
    /// the replacer and add the frame back to the free list. Also, reset
    /// the page's memory and metadata. Finally, you should call
    /// DeallocatePage() to imitate freeing the page on the disk.
    ///
    /// @param page_id id of page to be deleted
    /// @return false if the page exists but could not be deleted, true if the
    /// page didn't exist or deletion succeeded
    pub fn delete_page(&mut self, page_id: PageId) -> bool {
        if let Some(frame_id) = self.page_table.lock().unwrap().get(&page_id) {
            let page = &self.pages[*frame_id];
            if page.get_pin_count() > 0 {
                return false;
            }
            self.page_table.lock().unwrap().remove(&page_id);
            self.replacer.remove(*frame_id);
            self.free_list.lock().unwrap().push(*frame_id);
            page.reset();
            self.deallocate_page(page_id);
            true
        } else {
            true
        }
    }

    /// @brief Allocate a page on disk. Caller should acquire the latch before
    /// calling this function. @return the id of the allocated page
    fn allocate_page(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::SeqCst) as PageId
    }

    /// @brief Deallocate a page on disk. Caller should acquire the latch before
    /// calling this function. @param page_id id of the page to deallocate
    fn deallocate_page(&self, page_id: PageId) {
        // This is a no-nop right now without a more complex data structure to
        // track deallocated pages
    }

    // TODO(student): You may add additional private members and helper functions
}

mod tests {
    use std::fs;

    use rand::distributions::{Distribution, Uniform};
    use tempdir::TempDir;

    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::storage::disk::DiskManager;

    const BUSTUB_PAGE_SIZE: usize = 4096; // Placeholder for actual page size

    #[test]
    fn test_buffer_pool_manager_binary_data() {
        let dir = TempDir::new("test").unwrap();
        let db_name = dir.path().join("test.db");
        let buffer_pool_size = 10;
        let k = 5;

        let mut rng = rand::thread_rng();
        let uniform_dist = Uniform::from(std::u8::MIN..=std::u8::MAX);

        let disk_manager = DiskManager::new(db_name.to_str().unwrap());
        let mut bpm = BufferPoolManager::new(buffer_pool_size, disk_manager, k);

        let page0 = bpm.new_page();

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        assert!(page0.is_some());

        // Generate random binary data
        let mut random_binary_data: Vec<u8> = (0..BUSTUB_PAGE_SIZE)
            .map(|_| uniform_dist.sample(&mut rng))
            .collect();

        // Insert terminal characters both in the middle and at end
        random_binary_data[BUSTUB_PAGE_SIZE / 2] = 0;
        random_binary_data[BUSTUB_PAGE_SIZE - 1] = 0;

        // Scenario: Once we have a page, we should be able to read and write content.
        let page0 = page0.unwrap();
        page0.get_data_mut()[..random_binary_data.len()].copy_from_slice(&random_binary_data);
        assert_eq!(
            random_binary_data,
            page0.get_data()[..random_binary_data.len()]
        );

        // Scenario: We should be able to create new pages until we fill up the buffer
        // pool.
        for _i in 1..buffer_pool_size {
            assert!(bpm.new_page().is_some());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any
        // new pages.
        for _i in buffer_pool_size..buffer_pool_size * 2 {
            assert!(bpm.new_page().is_none());
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create
        // 5 new pages
        for i in 0..5 {
            assert!(bpm.unpin_page(i, true));
            bpm.flush_page(i);
        }
        for _i in 0..5 {
            let page = bpm.new_page();
            assert!(page.is_some());
            // Unpin the page here to allow future fetching
            bpm.unpin_page(page.unwrap().get_page_id().unwrap(), false);
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        let page0 = bpm.fetch_page(0);
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        assert_eq!(*page0.get_data(), random_binary_data.as_slice());
        assert!(bpm.unpin_page(0, true));

        // Shutdown the disk manager and remove the temporary file we created.
        drop(bpm);
    }

    #[test]
    fn test_buffer_pool_manager_sample() {
        let dir = TempDir::new("test.db").unwrap();
        let db_name = dir.path().join("test.db");
        let buffer_pool_size = 10;
        let k = 5;

        let disk_manager = DiskManager::new(db_name.to_str().unwrap());
        let mut bpm = BufferPoolManager::new(buffer_pool_size, disk_manager, k);

        let page0 = bpm.new_page();

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        assert!(page0.is_some());
        assert_eq!(0, page0.as_ref().unwrap().get_page_id().unwrap());

        // Scenario: Once we have a page, we should be able to read and write content.
        let page0 = page0.unwrap();
        let data = "Hello".as_bytes();
        page0.get_data_mut()[..data.len()].copy_from_slice(data);
        assert_eq!(data, &(page0.get_data())[..data.len()]);

        // Scenario: We should be able to create new pages until we fill up the buffer
        // pool.
        for i in 1..buffer_pool_size {
            assert!(bpm.new_page().is_some());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any
        // new pages.
        for _i in buffer_pool_size..buffer_pool_size * 2 {
            assert!(bpm.new_page().is_none());
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new
        // pages, there would still be one buffer page left for reading page 0.
        for i in 0..5 {
            assert_eq!(true, bpm.unpin_page(i as PageId, true));
        }
        for _i in 0..4 {
            assert!(bpm.new_page().is_some());
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        let page0 = bpm.fetch_page(0);
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        assert_eq!(data, &(page0.get_data())[..data.len()]);

        // Scenario: If we unpin page 0 and then make a new page, all the buffer pages
        // should now be pinned. Fetching page 0 again should fail.
        assert_eq!(true, bpm.unpin_page(0, true));
        assert!(bpm.new_page().is_some());
        assert!(bpm.fetch_page(0).is_none());

        // Shutdown the disk manager and remove the temporary file we created.
        // Replace this with the actual method to shut down the disk manager.
        drop(bpm);
    }
}
