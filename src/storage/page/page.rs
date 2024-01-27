use std::sync::Arc;

use parking_lot::{
    MappedMutexGuard, MappedRwLockReadGuard, MappedRwLockWriteGuard, MutexGuard, RwLock,
    RwLockReadGuard, RwLockWriteGuard,
};

use crate::common::config::{Lsn, PageId, BUSTUB_PAGE_SIZE};

const SIZE_PAGE_HEADER: usize = 8;
const OFFSET_PAGE_START: usize = 0;
const OFFSET_LSN: usize = 4;

/// Page is the basic unit of storage within the database system. Page provides
/// a wrapper for actual data pages being held in main memory. Page also
/// contains book-keeping information that is used by the buffer pool manager,
/// e.g. pin count, dirty flag, page id, etc.
#[derive(Debug, Clone)]
pub struct Page(Arc<RwLock<PageInner>>);

#[derive(Debug)]
struct PageInner {
    // The actual data that is stored within a page.
    data: [u8; BUSTUB_PAGE_SIZE],

    // The ID of this page.
    page_id: Option<PageId>,

    // The pin count of this page.
    pin_count: i32,

    // True if the page is dirty, i.e. it is different from its corresponding page on disk.
    is_dirty: bool,
}

impl Page {
    /// Constructor. Zeros out the page data.
    pub fn new() -> Page {
        let inner = PageInner {
            data: [0; BUSTUB_PAGE_SIZE],
            page_id: None,
            pin_count: 0,
            is_dirty: false,
        };
        Page(Arc::new(RwLock::new(inner)))
    }

    pub fn reset(&self) {
        let mut p = self.0.write();
        p.data.fill(0);
        p.page_id = None;
        p.pin_count = 0;
        p.is_dirty = false;
    }

    /// @return the actual data contained within this page
    pub fn get_data(&self) -> MappedRwLockReadGuard<'_, [u8; BUSTUB_PAGE_SIZE]> {
        RwLockReadGuard::map(self.0.read(), |i| &i.data)
    }
    pub fn get_mut_data(&self) -> MappedRwLockWriteGuard<'_, [u8; BUSTUB_PAGE_SIZE]> {
        RwLockWriteGuard::map(self.0.write(), |i| &mut i.data)
    }

    pub fn set_page_id(&self, page_id: PageId) {
        self.0.write().page_id = Some(page_id);
    }

    /// @return the page id of this page
    pub fn get_page_id(&self) -> Option<PageId> {
        self.0.read().page_id
    }

    /// @return the pin count of this page
    pub fn get_pin_count(&self) -> i32 {
        self.0.read().pin_count
    }

    pub fn pin(&self) {
        self.0.write().pin_count += 1;
    }

    pub fn unpin(&self) {
        self.0.write().pin_count -= 1;
    }

    /// @return true if the page in memory has been modified from the page on
    /// disk, false otherwise
    pub fn is_dirty(&self) -> bool {
        self.0.read().is_dirty
    }

    pub fn set_dirty(&self, is_dirty: bool) {
        self.0.write().is_dirty = is_dirty;
    }

    /// @return the page LSN.
    // This method assumes that LSN is stored at a certain offset in the data.
    pub fn get_lsn(&self) -> Lsn {
        let inner = self.0.read();
        Lsn::from_ne_bytes(
            inner.data[OFFSET_LSN..OFFSET_LSN + std::mem::size_of::<Lsn>()]
                .try_into()
                .unwrap(),
        )
    }

    /// Sets the page LSN.
    pub fn set_lsn(&self, lsn: Lsn) {
        let mut inner = self.0.write();
        let lsn_bytes = lsn.to_ne_bytes();
        inner.data[OFFSET_LSN..OFFSET_LSN + std::mem::size_of::<Lsn>()].copy_from_slice(&lsn_bytes);
    }
}
