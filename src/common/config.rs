// 数据页的大小（字节）
pub const BUSTUB_PAGE_SIZE: usize = 4096;
pub const INVALID_PAGE_ID: PageId = std::u32::MAX;

// table heap对应的缓冲池的大小（页）
pub const TABLE_HEAP_BUFFER_POOL_SIZE: usize = 100;
pub const LRUK_REPLACER_K: usize = 10; // lookback window for lru-k replacer

pub type FrameId = usize; // frame id type
pub type PageId = u32; // page id type
pub type TransactionId = u32; // transaction id type
pub type Lsn = u64; // log sequence number type
