use std::sync::{Arc, RwLock};

use crate::common::config::{PageId, BUSTUB_PAGE_SIZE};

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
    // Usually this should be stored as `char data_[BUSTUB_PAGE_SIZE]{};`. But to enable ASAN to
    // detect page overflow, we store it as a ptr.
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

    /// Zeroes out the data that is held within the page.
    fn reset_memory(&mut self) {
        for byte in &mut self.data {
            *byte = 0;
        }
    }

    /// @return the actual data contained within this page
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.0.write().unwrap().page_id = Some(page_id);
    }

    /// @return the page id of this page
    pub fn get_page_id(&self) -> Option<PageId> {
        self.0.read().unwrap().page_id
    }

    /// @return the pin count of this page
    pub fn get_pin_count(&self) -> i32 {
        self.0.read().unwrap().pin_count
    }

    /// @return true if the page in memory has been modified from the page on
    /// disk, false otherwise
    pub fn is_dirty(&self) -> bool {
        self.0.read().unwrap().is_dirty
    }

    /// @return the page LSN.
    // This method assumes that LSN is stored at a certain offset in the data.
    pub fn get_lsn(&self) -> Lsn {
        let lsn_bytes = &self.data[OFFSET_LSN..OFFSET_LSN + std::mem::size_of::<Lsn>()];
        Lsn::from_ne_bytes(lsn_bytes.try_into().unwrap()) // Assuming Lsn is a defined type
    }

    /// Sets the page LSN.
    pub fn set_lsn(&mut self, lsn: Lsn) {
        let lsn_bytes = lsn.to_ne_bytes();
        self.data[OFFSET_LSN..OFFSET_LSN + std::mem::size_of::<Lsn>()].copy_from_slice(&lsn_bytes);
    }
}
