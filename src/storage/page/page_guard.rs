use std::sync::Arc;

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::config::PageId;
use crate::storage::page::page::{MutRefPageData, Page, RefPageData};

pub struct BasicPageGuard {
    bpm: Arc<BufferPoolManager>,
    page: Page,
    is_dirty: bool,
}

impl BasicPageGuard {
    pub fn new(bpm: Arc<BufferPoolManager>, page: Page) -> BasicPageGuard {
        Self {
            bpm,
            page,
            is_dirty: false,
        }
    }

    /// TODO(P2): Add implementation
    ///
    /// @brief Drop a page guard
    ///
    /// Dropping a page guard should clear all contents
    /// (so that the page guard is no longer useful), and
    /// it should tell the BPM that we are done using this page,
    /// per the specification in the writeup.
    pub fn drop(&self) {
        unimplemented!()
    }

    /// TODO(P2): Add implementation
    ///
    /// @brief Upgrade a BasicPageGuard to a ReadPageGuard
    ///
    /// The protected page is not evicted from the buffer pool during the
    /// upgrade, and the basic page guard should be made invalid after
    /// calling this function.
    ///
    /// @return an upgraded ReadPageGuard
    pub fn upgrade_read(self) -> ReadPageGuard {
        unimplemented!()
    }

    /// TODO(P2): Add implementation
    ///
    /// @brief Upgrade a BasicPageGuard to a WritePageGuard
    ///
    /// The protected page is not evicted from the buffer pool during the
    /// upgrade, and the basic page guard should be made invalid after
    /// calling this function.
    ///
    /// @return an upgraded WritePageGuard
    pub fn upgrade_write(self) -> WritePageGuard {
        unimplemented!()
    }

    pub fn page_id(&self) -> PageId {
        self.page.get_page_id().unwrap()
    }

    pub fn get_data(&self) -> RefPageData {
        self.page.get_data()
    }

    pub fn get_data_mut(&mut self) -> MutRefPageData {
        self.is_dirty = true;
        self.page.get_data_mut()
    }
}

impl Drop for BasicPageGuard {
    /// TODO(P2): Add implementation
    ///
    /// @brief Drop a page guard
    ///
    /// Dropping a page guard should clear all contents
    /// (so that the page guard is no longer useful), and
    /// it should tell the BPM that we are done using this page,
    /// per the specification in the writeup.
    fn drop(&mut self) {
        unimplemented!()
    }
}

pub struct ReadPageGuard {
    // You may choose to get rid of this and add your own private variables.
    guard: BasicPageGuard,
}

impl ReadPageGuard {
    pub fn new(bpm: Arc<BufferPoolManager>, page: Page) -> Self {
        Self {
            guard: BasicPageGuard::new(bpm, page),
        }
    }

    /// TODO(P2): Add implementation
    ///
    /// @brief Drop a ReadPageGuard
    ///
    /// ReadPageGuard's Drop should behave similarly to BasicPageGuard,
    /// except that ReadPageGuard has an additional resource - the latch!
    /// However, you should think VERY carefully about in which order you
    /// want to release these resources.
    pub fn drop(&mut self) {
        unimplemented!()
    }

    pub fn page_id(&self) -> PageId {
        self.guard.page_id()
    }

    /// Retrieves the data from the page
    pub fn get_data(&self) -> RefPageData {
        self.guard.get_data()
    }
}

impl Drop for ReadPageGuard {
    /// TODO(P2): Add implementation
    ///
    /// @brief Destructor for ReadPageGuard
    ///
    /// Just like with BasicPageGuard, this should behave
    /// as if you were dropping the guard.
    fn drop(&mut self) {
        unimplemented!()
    }
}

pub struct WritePageGuard {
    // You may choose to get rid of this and add your own private variables.
    guard: BasicPageGuard,
}

impl WritePageGuard {
    pub fn new(bpm: Arc<BufferPoolManager>, page: Page) -> Self {
        WritePageGuard {
            guard: BasicPageGuard::new(bpm, page),
        }
    }

    /// TODO(P2): Add implementation
    ///
    /// @brief Drop a WritePageGuard
    ///
    /// WritePageGuard's Drop should behave similarly to BasicPageGuard,
    /// except that WritePageGuard has an additional resource - the latch!
    pub fn drop(&mut self) {
        unimplemented!()
    }

    pub fn page_id(&self) -> PageId {
        self.guard.page_id()
    }

    pub fn get_data(&self) -> RefPageData {
        self.guard.get_data()
    }

    pub fn get_data_mut(&mut self) -> MutRefPageData {
        self.guard.get_data_mut()
    }
}

impl Drop for WritePageGuard {
    /// TODO(P2): Add implementation
    ///
    /// @brief Destructor for WritePageGuard
    ///
    /// Just like with BasicPageGuard, this should behave
    /// as if you were dropping the guard.
    fn drop(&mut self) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use crate::storage::disk::disk_manager::DiskManager;
    #[test]
    fn test_page_guard_sample() {
        let buffer_pool_size = 5;
        let k = 2;

        let dir = TempDir::new("test").unwrap();
        let db_file = dir.path().join("test.db");
        let disk_manager = DiskManager::new(db_file.to_str().unwrap());
        let bpm = Arc::new(BufferPoolManager::new(buffer_pool_size, disk_manager, k));

        let page0 = bpm.new_page().unwrap();

        let guarded_page = BasicPageGuard::new(bpm.clone(), page0.clone());

        assert_eq!(*page0.get_data(), *guarded_page.get_data());
        assert_eq!(page0.get_page_id(), Some(guarded_page.page_id()));
        assert_eq!(1, page0.get_pin_count());

        guarded_page.drop();

        assert_eq!(0, page0.get_pin_count());

        {
            let page2 = bpm.new_page().unwrap();
            let guard1 = page2.get_data();
            let guard2 = ReadPageGuard::new(bpm.clone(), page2.clone());
        }

        // Shutdown the disk manager and remove the temporary file we created.
        drop(bpm);
    }
}
