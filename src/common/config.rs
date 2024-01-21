// 数据页的大小（字节）
pub const BUSTUB_PAGE_SIZE: usize = 4096;
pub const INVALID_PAGE_ID: PageId = std::u32::MAX;

// table heap对应的缓冲池的大小（页）
pub const TABLE_HEAP_BUFFER_POOL_SIZE: usize = 100;

pub type FrameId = u32;
pub type PageId = u32;
pub type TransactionId = u32;
