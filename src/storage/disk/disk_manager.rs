use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Mutex,
};

use log::debug;

use crate::common::config::{PageId, BUSTUB_PAGE_SIZE};

/// DiskManager takes care of the allocation and deallocation of pages within a
/// database. It performs the reading and writing of pages to and from disk,
/// providing a logical file layer within the context of a database management
/// system.
struct DiskManager {
    // Stream to write log file
    log_io: File,
    log_name: String,
    // Stream to write db file
    // Protects file access with multiple buffer pool instances
    db_io: Mutex<File>,
    file_name: String,
    // Number of disk flushes
    num_flushes: i32,
    // Number of disk writes
    num_writes: i32,
    // Indicates if the in-memory content has not been flushed yet
    flush_log: bool,
    // Future for non-blocking flushes
    flush_log_f: Option<Box<dyn Future<Output = ()>>>,
}

impl DiskManager {
    /// Creates a new disk manager that writes to the specified database file.
    fn new(db_file: &str) -> Self {
        // Extract the base file name and add ".log" extension for the log file
        let file_name = Path::new(db_file);
        let log_name = file_name.with_extension("log");

        // Try to open the log file, create it if it doesn't exist
        let log_io = OpenOptions::new()
            .read(true)
            .append(true)
            .write(true)
            .open(&log_name)
            .or_else(|_| {
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&log_name)
            })
            .unwrap();

        // Ensure the db file is open, create it if it doesn't exist
        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_file)
            .or_else(|_| {
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(db_file)
            })
            .unwrap();

        Self {
            log_io,
            log_name: log_name.to_string_lossy().to_string(),
            db_io: Mutex::new(db_io),
            file_name: db_file.to_string(),
            num_flushes: 0,
            num_writes: 0,
            flush_log: false,
            flush_log_f: None,
        }
    }

    /// FOR TEST / LEADERBOARD ONLY, used by DiskManagerMemory
    fn default() -> Self {
        unimplemented!()
    }

    /// Write a page to the database file.
    fn write_page(&mut self, page_id: PageId, page_data: &[u8]) {
        assert_eq!(page_data.len(), BUSTUB_PAGE_SIZE);

        let offset = page_id as usize * BUSTUB_PAGE_SIZE;
        // set write cursor to offset
        self.num_writes += 1;

        let mut db_io = self.db_io.lock().unwrap();
        db_io.seek(SeekFrom::Start(offset as u64)).unwrap();
        if let Err(e) = db_io.write_all(&page_data) {
            panic!("I/O error while writing: {:?}", e);
        }
        // needs to flush to keep disk file in sync
        db_io.flush().unwrap();
    }

    /// Read a page from the database file.
    fn read_page(&mut self, page_id: PageId, page_data: &mut [u8]) {
        let offset = page_id as usize * BUSTUB_PAGE_SIZE;

        let mut db_io = self.db_io.lock().unwrap();
        // check if read beyond file length
        if offset > db_io.metadata().unwrap().len() as usize {
            panic!("I/O error reading past end of file");
        }
        // set read cursor to offset
        db_io.seek(SeekFrom::Start(offset as u64)).unwrap();
        assert_eq!(page_data.len(), BUSTUB_PAGE_SIZE);
        match db_io.read(page_data) {
            Ok(read_count) => {
                // if file ends before reading BUSTUB_PAGE_SIZE
                if read_count < BUSTUB_PAGE_SIZE {
                    debug!("Read less than a page");
                    // fill the rest of the buffer with 0
                    page_data[read_count..].fill(0);
                }
            }
            Err(e) => panic!("I/O error while reading: {:?}", e),
        };
    }

    /// Write the contents of the log into disk file
    /// Only return when sync is done, and only perform sequence write
    fn write_log(&mut self, log_data: &[u8]) {
        if log_data.is_empty() {
            // no effect on num_flushes_ if log buffer is empty
            return;
        }

        self.flush_log = true;

        if let Some(_f) = &self.flush_log_f {
            // used for checking non-blocking flushing
            // assert(flush_log_f_->wait_for(std::chrono::seconds(10)) ==
            //     std::future_status::ready);
            unimplemented!();
        }

        self.num_flushes += 1;
        // sequence write
        if let Err(e) = self.log_io.write_all(log_data) {
            // check for I/O error
            panic!("I/O error while writing log: {:?}", e);
        }
        // needs to flush to keep disk file in sync
        self.log_io.flush().unwrap();
        self.flush_log = false;
    }

    /// Read the contents of the log into the given memory area
    /// Always read from the beginning and perform sequence read
    /// @return: false means already reach the end
    fn read_log(&mut self, log_data: &mut [u8], offset: usize) -> bool {
        if offset >= self.log_io.metadata().unwrap().len() as usize {
            debug!("Read past end of log file");
            debug!("file size is {}", self.log_io.metadata().unwrap().len());
            return false;
        }
        self.log_io.seek(SeekFrom::Start(offset as u64)).unwrap();
        match self.log_io.read(log_data) {
            Ok(read_count) => {
                // if file ends before reading BUSTUB_PAGE_SIZE
                if read_count < BUSTUB_PAGE_SIZE {
                    debug!("Read less than a page");
                    // fill the rest of the buffer with 0
                    log_data[read_count..].fill(0);
                }
            }
            Err(e) => panic!("I/O error while reading log: {:?}", e),
        };
        true
    }

    /// Returns the number of disk flushes.
    fn get_num_flushes(&self) -> i32 {
        self.num_flushes
    }

    /// Returns true if the in-memory content has not been flushed yet.
    fn get_flush_state(&self) -> bool {
        self.flush_log
    }

    /// Returns the number of disk writes.
    fn get_num_writes(&self) -> i32 {
        self.num_writes
    }

    /// Sets the future which is used to check for non-blocking flushes.
    fn set_flush_log_future(&mut self, f: Box<dyn Future<Output = ()>>) {
        self.flush_log_f = Some(f);
    }

    /// Checks if the non-blocking flush future was set.
    fn has_flush_log_future(&self) -> bool {
        self.flush_log_f.is_some()
    }
}

mod tests {
    use tempdir::TempDir;

    use super::*;
    use crate::common::config::BUSTUB_PAGE_SIZE;

    #[test]
    fn read_write_page() {
        let mut buf = [0; BUSTUB_PAGE_SIZE];
        let mut data = [0; BUSTUB_PAGE_SIZE];

        let dir = TempDir::new("test").unwrap();
        let db_file = dir.path().join("test.db");
        let mut dm = DiskManager::new(db_file.to_str().unwrap());
        let test_str = b"A test string.";
        data[..test_str.len()].copy_from_slice(test_str);

        dm.read_page(0, &mut buf); // tolerate empty read

        dm.write_page(0, &data);
        dm.read_page(0, &mut buf);
        assert_eq!(buf, data);

        buf.fill(0);
        dm.write_page(5, &data);
        dm.read_page(5, &mut buf);
        assert_eq!(buf, data);
    }

    #[test]
    fn read_write_log() {
        let mut buf = [0; 14];

        let dir = TempDir::new("test").unwrap();
        let db_file = dir.path().join("test.db");
        let mut dm = DiskManager::new(db_file.to_str().unwrap());
        let test_str = b"A test string.";

        dm.read_log(&mut buf, 0); // tolerate empty read

        dm.write_log(test_str);
        dm.read_log(&mut buf, 0);
        assert_eq!(&buf, test_str);
    }
}
