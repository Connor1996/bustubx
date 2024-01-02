use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
};

use super::lru_k_replacer::LRUKReplacer;
use crate::{
    common::config::BUSTUBX_PAGE_SIZE,
    storage::{
        disk_manager::DiskManager,
        page::{Page, PageId},
    },
};

pub type FrameId = u32;

/// BufferPoolManager reads disk pages to and from its internal buffer pool.
pub struct BufferPoolManager {
    /// Number of pages in the buffer pool.
    pool_size: usize,
    /// The next page id to be allocated
    next_page_id: AtomicUsize,

    /// Array of buffer pool pages.
    pages: Vec<Page>, // Assuming Page is a struct representing a page
    /// Pointer to the disk scheduler.
    disk_scheduler: Arc<DiskScheduler>, // Assuming DiskScheduler is a struct
    /// Pointer to the log manager. Please ignore this for P1.
    log_manager: Option<Arc<LogManager>>, // Optional for enabling/disabling logging
    /// Page table for keeping track of buffer pool pages.
    page_table: Mutex<HashMap<PageId, FrameId>>,
    /// Replacer to find unpinned pages for replacement.
    replacer: LRUKReplacer, // Assuming LRUKReplacer is a struct
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
        disk_manager: Arc<DiskManager>,
        replacer_k: usize, // Assuming the default is defined elsewhere
        log_manager: Option<Arc<LogManager>>,
    ) -> BufferPoolManager {
        unimplemented!()
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
    /// page's id, or nullptr if all frames are currently in use and not
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
    /// @param[out] page_id id of created page
    /// @return nullptr if no new pages could be created, otherwise pointer to
    /// new page
    pub fn new_page(&mut self, page_id: &mut Option<PageId>) -> Option<&Page> {
        unimplemented!()
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
    /// @brief Fetch the requested page from the buffer pool. Return nullptr if
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
    /// @param access_type type of access to the page, only needed for
    /// leaderboard tests. @return nullptr if page_id cannot be fetched,
    /// otherwise pointer to the requested page
    pub fn fetch_page(&mut self, page_id: PageId, access_type: AccessType) -> Option<&Page> {
        unimplemented!()
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
    /// otherwise @param access_type type of access to the page, only needed
    /// for leaderboard tests. @return false if the page is not in the page
    /// table or its pin count is <= 0 before this call, true otherwise
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool, access_type: AccessType) -> bool {
        unimplemented!()
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
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Flush all the pages in the buffer pool to disk.
    pub fn flush_all_pages(&mut self) {
        unimplemented!()
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
    pub fn delete_page(&mut self, page_id: bool) {
        unimplemented!()
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
        unimplemented!()
    }

    // TODO(student): You may add additional private members and helper functions
}

mod tests {
    use std::{fs, fs::remove_file, sync::Arc};

    use rand::{distributions::Uniform, Rng};

    use super::*;
    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager, storage::disk_manager::DiskManager,
    };

    const BUSTUB_PAGE_SIZE: usize = 4096; // Placeholder for actual page size

    #[test]
    fn test_buffer_pool_manager_binary_data() {
        let db_name = TempDir::new("test.db");
        let buffer_pool_size = 10;
        let k = 5;

        let mut rng = rand::thread_rng();
        let uniform_dist = Uniform::from(std::i8::MIN..=std::i8::MAX);

        let disk_manager = DiskManager::new(db_name);
        let mut bpm = BufferPoolManager::new(buffer_pool_size, disk_manager, k, None); 

        let mut page_id_temp: PageId = 0; // PageId should be the type your system uses for page IDs
        let page0 = bpm.new_page(&mut page_id_temp);

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        assert!(page0.is_some());
        assert_eq!(0, page_id_temp);

        // Generate random binary data
        let mut random_binary_data: Vec<u8> = (0..BUSTUB_PAGE_SIZE)
            .map(|_| uniform_dist.sample(&mut rng) as u8)
            .collect();

        // Insert terminal characters both in the middle and at end
        random_binary_data[BUSTUB_PAGE_SIZE / 2] = 0;
        random_binary_data[BUSTUB_PAGE_SIZE - 1] = 0;

        // Scenario: Once we have a page, we should be able to read and write content.
        let page0 = page0.unwrap(); // Unwrap to use the page
        page0.set_data(&random_binary_data); // Replace with actual method to write data to a page
        assert_eq!(page0.get_data(), &random_binary_data); // Replace with actual method to read data from a page

        // Scenario: We should be able to create new pages until we fill up the buffer
        // pool.
        for _i in 1..buffer_pool_size {
            assert!(bpm.new_page(&mut page_id_temp).is_some());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any
        // new pages.
        for _i in buffer_pool_size..buffer_pool_size * 2 {
            assert!(bpm.new_page(&mut page_id_temp).is_none());
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create
        // 5 new pages
        for i in 0..5 {
            assert!(bpm.unpin_page(i, true));
            bpm.flush_page(i);
        }
        for _i in 0..5 {
            assert!(bpm.new_page(&mut page_id_temp).is_some());
            bpm.unpin_page(page_id_temp, false); // Unpin the page here to allow future fetching
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        let page0 = bpm.fetch_page(0);
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        assert_eq!(page0.get_data(), &random_binary_data);
        assert!(bpm.unpin_page(0, true));

        // Shutdown the disk manager and remove the temporary file we created.
        disk_manager.shutdown(); // Replace with actual shutdown method
        fs::remove_file("test.db").expect("Failed to remove test file");
    }

    #[test]
    fn test_buffer_pool_manager_sample() {
        let db_name = TempDir::new("test.db");
        let buffer_pool_size = 10;
        let k = 5;

        let disk_manager = Box::new(DiskManager::new(db_name));
        let mut bpm = Box::new(BufferPoolManager::new(
            buffer_pool_size,
            Arc::new(disk_manager),
            k,
            None,
        ));

        let mut page_id_temp = 0;
        let page0 = bpm.new_page(&mut page_id_temp);

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        assert!(page0.is_some());
        assert_eq!(0, page_id_temp);

        // Scenario: Once we have a page, we should be able to read and write content.
        let mut page0 = page0.unwrap(); // Unwrap to use the page, handle None case as needed
        page0.set_data("Hello".as_bytes()); // Replace with actual method to write data to a page
        assert_eq!("Hello".as_bytes(), page0.get_data()); // Replace with actual method to read data from a page

        // Scenario: We should be able to create new pages until we fill up the buffer
        // pool.
        for i in 1..buffer_pool_size {
            assert!(bpm.new_page(&mut page_id_temp).is_some());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any
        // new pages.
        for _i in buffer_pool_size..buffer_pool_size * 2 {
            assert!(bpm.new_page(&mut page_id_temp).is_none());
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new
        // pages, there would still be one buffer page left for reading page 0.
        for i in 0..5 {
            assert_eq!(true, bpm.unpin_page(i as PageId, true)); // Assuming PageId is the type for page ids
        }
        for _i in 0..4 {
            assert!(bpm.new_page(&mut page_id_temp).is_some());
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        let page0 = bpm.fetch_page(0); // Assuming fetch_page takes a PageId and returns an Option<Page>
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        assert_eq!("Hello".as_bytes(), page0.get_data());

        // Scenario: If we unpin page 0 and then make a new page, all the buffer pages
        // should now be pinned. Fetching page 0 again should fail.
        assert_eq!(true, bpm.unpin_page(0, true));
        assert!(bpm.new_page(&mut page_id_temp).is_some());
        assert!(bpm.fetch_page(0).is_none());

        // Shutdown the disk manager and remove the temporary file we created.
        // Replace this with the actual method to shut down the disk manager.
        disk_manager.shutdown();
        std::fs::remove_file("test.db").expect("Failed to remove test file");

        // Implicit drop calls for bpm and disk_manager here
    }
}
