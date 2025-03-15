#![allow(dead_code)]

use std::{
    collections::HashMap,
    error::Error,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
mod test_utils;
#[cfg(test)]
use test_utils::TestDir;

/// The database struct
struct SimpleDB {
    db_directory: PathBuf,
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl SimpleDB {
    const LOG_FILE: &str = "simpledb.log";

    fn new<P: AsRef<Path>>(path: P, block_size: usize, num_buffers: usize) -> Self {
        let file_manager = Arc::new(Mutex::new(FileManager::new(&path, block_size).unwrap()));
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&file_manager),
            Self::LOG_FILE,
        )));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            num_buffers,
        )));
        Self {
            db_directory: path.as_ref().to_path_buf(),
            log_manager,
            file_manager,
            buffer_manager,
        }
    }

    #[cfg(test)]
    fn new_for_test(block_size: usize, num_buffers: usize) -> (Self, TestDir) {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let thread_id = std::thread::current().id();
        let test_dir = TestDir::new(format!("/tmp/test_db_{}_{:?}", timestamp, thread_id));
        let db = Self::new(&test_dir, block_size, num_buffers);
        (db, test_dir)
    }
}

struct Buffer {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    contents: Page,
    block_id: Option<BlockId>,
    pins: usize,
    txn: Option<usize>,
    lsn: Option<usize>,
}

impl Buffer {
    fn new(file_manager: Arc<Mutex<FileManager>>, log_manager: Arc<Mutex<LogManager>>) -> Self {
        let size = file_manager.lock().unwrap().blocksize;
        Self {
            file_manager,
            log_manager,
            contents: Page::new(size),
            block_id: None,
            pins: 0,
            txn: None,
            lsn: None,
        }
    }

    /// Mark that this buffer has been modified and set associated metadata for the modifying transaction
    fn set_modified(&mut self, txn_num: usize, lsn: usize) {
        self.txn = Some(txn_num);
        self.lsn = Some(lsn);
    }

    /// Check whether the buffer is pinned in memory
    fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    /// Modify this buffer to hold the contents of a different block
    /// This requires flushing the existing page contents, if any, to disk if dirty
    fn assign_to_block(&mut self, block_id: &BlockId) {
        self.flush();
        self.block_id = Some(block_id.clone());
        self.file_manager
            .lock()
            .unwrap()
            .read(self.block_id.as_ref().unwrap(), &mut self.contents);
        self.reset_pins();
    }

    /// Write the current buffer contents to disk if dirty
    fn flush(&mut self) {
        if let Some(_) = &self.txn {
            self.log_manager
                .lock()
                .unwrap()
                .flush_lsn(self.lsn.unwrap());
            self.file_manager
                .lock()
                .unwrap()
                .write(self.block_id.as_ref().unwrap(), &mut self.contents);
        }
    }

    /// Increment the pin count for this buffer
    fn pin(&mut self) {
        self.pins += 1;
    }

    /// Decrement the pin count for this buffer
    fn unpin(&mut self) {
        assert!(self.pins > 0); //  sanity check to know that it will not become negative
        self.pins -= 1;
    }

    /// Reset the pin count for this buffer
    fn reset_pins(&mut self) {
        self.pins = 0;
    }
}

struct BufferManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_pool: Vec<Arc<Mutex<Buffer>>>,
    num_available: Mutex<usize>,
    cond: Condvar,
}

impl BufferManager {
    const MAX_TIME: u64 = 10; //  10 seconds
    fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        num_buffers: usize,
    ) -> Self {
        let buffer_pool = (0..num_buffers)
            .map(|_| {
                Arc::new(Mutex::new(Buffer::new(
                    Arc::clone(&file_manager),
                    Arc::clone(&log_manager),
                )))
            })
            .collect();
        Self {
            file_manager,
            log_manager,
            buffer_pool,
            num_available: Mutex::new(num_buffers),
            cond: Condvar::new(),
        }
    }

    /// Returns the number of unpinned buffers, that is buffers with no pages pinned to them
    fn available(&self) -> usize {
        *self.num_available.lock().unwrap()
    }

    /// Flushes the dirty buffers modified by this specific transaction
    fn flush_all(&mut self, txn_num: usize) {
        for buffer in &mut self.buffer_pool {
            let mut buffer = buffer.lock().unwrap();
            if buffer.txn.is_some() && *buffer.txn.as_ref().unwrap() == txn_num {
                buffer.flush();
            }
        }
    }

    /// Pin the buffer associated with the provided block_id
    fn pin(&self, block_id: &BlockId) -> Result<Arc<Mutex<Buffer>>, Box<dyn Error>> {
        let start = Instant::now();
        let mut num_available = self.num_available.lock().unwrap();
        loop {
            match self.try_to_pin(block_id) {
                Some(buffer) => {
                    {
                        let mut buffer_guard = buffer.lock().unwrap();
                        if !buffer_guard.is_pinned() {
                            buffer_guard.pin();
                            *num_available -= 1;
                        }
                    }
                    return Ok(buffer);
                }
                None => {
                    num_available = self.cond.wait(num_available).unwrap();
                    if start.elapsed() > Duration::from_secs(Self::MAX_TIME) {
                        return Err("Timed out waiting for buffer".into());
                    }
                }
            }
        }
    }

    /// Find a buffer to pin this block to
    /// First check to see if there is an existing buffer for this block
    /// If not, try to find an unpinned buffer
    /// If both cases above fail, return None
    /// Update matadata for the assigned buffer before returning
    fn try_to_pin(&self, block_id: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        let buffer = match self.find_existing_buffer(block_id) {
            Some(buffer) => buffer,
            None => match self.choose_unpinned_buffer() {
                Some(buffer) => {
                    buffer.lock().unwrap().assign_to_block(block_id);
                    buffer
                }
                None => return None,
            },
        };
        return Some(buffer);
    }

    /// Decrement the pin count for the provided buffer
    /// If all of the pins have been removed, managed metadata & notify waiting threads
    fn unpin(&self, buffer: Arc<Mutex<Buffer>>) {
        let mut buffer_guard = buffer.lock().unwrap();
        buffer_guard.unpin();
        if !buffer_guard.is_pinned() {
            *self.num_available.lock().unwrap() += 1;
            self.cond.notify_all();
        }
    }

    /// Look for a buffer associated with this specific [`BlockId`]
    fn find_existing_buffer(&self, block_id: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in &self.buffer_pool {
            let buffer_guard = buffer.lock().unwrap();
            if buffer_guard.block_id.is_some()
                && buffer_guard.block_id.as_ref().unwrap() == block_id
            {
                return Some(Arc::clone(&buffer));
            }
        }
        None
    }

    /// Try to find an unpinned buffer and return pointer to that, if present
    fn choose_unpinned_buffer(&self) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in &self.buffer_pool {
            let buffer_guard = buffer.lock().unwrap();
            if !buffer_guard.is_pinned() {
                return Some(Arc::clone(&buffer));
            }
        }
        None
    }
}

#[cfg(test)]
mod buffer_manager_tests {
    use crate::{BlockId, Page, SimpleDB};

    /// This test will assert that when the buffer pool swaps out a page from the buffer pool, it properly flushes those contents to disk
    /// and can then correctly read them back later
    #[test]
    fn test_buffer_replacement() {
        let (db, _test_dir) = SimpleDB::new_for_test(400, 3); // use 3 buffer slots
        let buffer_manager = db.buffer_manager;

        //  Initialize the file with enough data
        let block_id = BlockId::new("testfile".to_string(), 1);
        let mut page = Page::new(400);
        page.set_int(80, 1);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);

        let buffer_manager_guard = buffer_manager.lock().unwrap();

        //  Create a buffer for block 1 and modify it
        let buffer_1 = buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 1))
            .unwrap();
        buffer_1.lock().unwrap().contents.set_int(80, 100);
        buffer_1.lock().unwrap().set_modified(1, 0);
        buffer_manager_guard.unpin(buffer_1);

        //  force buffer replacement by pinning 3 new blocks
        let buffer_2 = buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 2))
            .unwrap();
        buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 3))
            .unwrap();
        buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 4))
            .unwrap();

        //  remove one of the buffers so block 1 can be read back in
        buffer_manager_guard.unpin(buffer_2);

        //  Read block 1 back from disk and verify it is the same
        let buffer_2 = buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 1))
            .unwrap();
        assert_eq!(buffer_2.lock().unwrap().contents.get_int(80), 100);
    }
}

struct LogIterator {
    file_manager: Arc<Mutex<FileManager>>,
    current_block: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl LogIterator {
    fn new(file_manager: Arc<Mutex<FileManager>>, current_block: BlockId) -> Self {
        let block_size = file_manager.lock().unwrap().blocksize;
        let mut page = Page::new(block_size);
        file_manager.lock().unwrap().read(&current_block, &mut page);
        let boundary = page.get_int(0) as usize;

        Self {
            file_manager,
            current_block,
            page,
            current_pos: boundary,
            boundary,
        }
    }

    fn move_to_block(&mut self) {
        self.file_manager
            .lock()
            .unwrap()
            .read(&self.current_block, &mut self.page);
        self.boundary = self.page.get_int(0) as usize;
        self.current_pos = self.boundary;
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.file_manager.lock().unwrap().blocksize {
            if self.current_block.block_num == 0 {
                return None; //  no more blocks
            }
            self.current_block = BlockId {
                filename: self.current_block.filename.to_string(),
                block_num: self.current_block.block_num - 1,
            };
            self.move_to_block();
        }
        //  Read the record
        let record = self.page.get_bytes(self.current_pos);
        self.current_pos += Page::INT_BYTES + record.len();
        Some(record)
    }
}

impl IntoIterator for LogManager {
    type Item = Vec<u8>;
    type IntoIter = LogIterator;

    fn into_iter(mut self) -> Self::IntoIter {
        self.iterator()
    }
}

#[cfg(test)]
mod log_manager_tests {
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    use crate::{LogManager, Page, SimpleDB};

    fn create_log_record(s: &str, n: usize) -> Vec<u8> {
        let string_bytes = s.as_bytes();
        let total_size = Page::INT_BYTES + string_bytes.len() + Page::INT_BYTES;
        let mut record = Vec::with_capacity(total_size);

        record
            .write_all(&(string_bytes.len() as i32).to_be_bytes())
            .unwrap();
        record.write_all(&string_bytes).unwrap();
        record.write_all(&n.to_be_bytes()).unwrap();
        record
    }

    fn create_log_records(log_manager: Arc<Mutex<LogManager>>, start: usize, end: usize) {
        println!("creating records");
        for i in start..=end {
            let record = create_log_record(&format!("record{i}"), i + 100);
            let lsn = log_manager.lock().unwrap().append(record);
            print!("{lsn} ");
        }
        println!("");
    }

    fn print_log_records(log_manager: Arc<Mutex<LogManager>>, message: &str) {
        println!("{message}");
        let iter = log_manager.lock().unwrap().iterator();

        for record in iter {
            let length = i32::from_be_bytes(record[..4].try_into().unwrap());
            let string = String::from_utf8(record[4..4 + length as usize].to_vec()).unwrap();
            let n = usize::from_be_bytes(record[4 + (length as usize)..].try_into().unwrap());
            println!("{string} {n}");
        }
    }

    #[test]
    fn test_log_manager() {
        let (db, _test_dir) = SimpleDB::new_for_test(400, 0);
        let log_manager = db.log_manager;

        print_log_records(Arc::clone(&log_manager), "The initial empty log file: ");
        create_log_records(Arc::clone(&log_manager), 1, 35);
        print_log_records(
            Arc::clone(&log_manager),
            "The log file now has these records:",
        );
        create_log_records(Arc::clone(&log_manager), 36, 70);
        log_manager.lock().unwrap().flush_lsn(65);
        print_log_records(log_manager, "The log file now has these records:");
    }
}

struct LogManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_file: String,
    log_page: Page,
    current_block: BlockId,
    latest_lsn: usize,
    last_saved_lsn: usize,
}

impl LogManager {
    fn new(file_manager: Arc<Mutex<FileManager>>, log_file: &str) -> Self {
        let bytes = vec![0; file_manager.lock().unwrap().blocksize];
        let mut log_page = Page::from_bytes(bytes);
        let log_size = file_manager.lock().unwrap().length(log_file.to_string());
        let current_block = if log_size == 0 {
            LogManager::append_new_block(&file_manager, log_file, &mut log_page)
        } else {
            let block = BlockId {
                filename: log_file.to_string(),
                block_num: log_size - 1,
            };
            file_manager.lock().unwrap().read(&block, &mut log_page);
            block
        };
        Self {
            file_manager,
            log_file: log_file.to_string(),
            log_page,
            current_block,
            latest_lsn: 0,
            last_saved_lsn: 0,
        }
    }

    /// Determine if this LSN has been flushed to disk, and flush it if it hasn't
    fn flush_lsn(&mut self, lsn: usize) {
        if self.last_saved_lsn >= lsn {
            return;
        }
        self.flush_to_disk();
    }

    /// Write the bytes from log_page to disk for the current_block
    /// Update the last_saved_lsn before returning
    fn flush_to_disk(&mut self) {
        self.file_manager
            .lock()
            .unwrap()
            .write(&self.current_block, &mut self.log_page);
        self.last_saved_lsn = self.latest_lsn;
    }

    /// Write the log_record to the log page
    /// First, check if there is enough space
    fn append(&mut self, log_record: Vec<u8>) -> usize {
        let mut boundary = self.log_page.get_int(0) as usize;
        let bytes_needed = log_record.len() + Page::INT_BYTES;
        if boundary.saturating_sub(bytes_needed) < Page::INT_BYTES {
            self.flush_to_disk();
            self.current_block = LogManager::append_new_block(
                &mut self.file_manager,
                &self.log_file,
                &mut self.log_page,
            );
            boundary = self.log_page.get_int(0) as usize;
        }

        let record_pos = boundary - bytes_needed;
        self.log_page.set_bytes(record_pos, &log_record);
        self.log_page.set_int(0, record_pos as i32);
        self.latest_lsn += 1;
        self.latest_lsn
    }

    /// Append a new block to the file maintained by the log manager
    /// This involves initializing a new block, writing a boundary pointer to it and writing the block to disk
    fn append_new_block(
        file_manager: &Arc<Mutex<FileManager>>,
        log_file: &str,
        log_page: &mut Page,
    ) -> BlockId {
        let block_id = file_manager.lock().unwrap().append(log_file.to_string());
        log_page.set_int(
            0,
            file_manager.lock().unwrap().blocksize.try_into().unwrap(),
        );
        file_manager.lock().unwrap().write(&block_id, log_page);
        block_id
    }

    fn iterator(&mut self) -> LogIterator {
        self.flush_to_disk();
        LogIterator::new(
            Arc::clone(&self.file_manager),
            BlockId::new(self.log_file.clone(), self.current_block.block_num),
        )
    }
}

/// The block id container that contains a specific block number for a specific file
#[derive(Debug, Eq, PartialEq, Clone)]
struct BlockId {
    filename: String,
    block_num: usize,
}

impl BlockId {
    fn new(filename: String, block_num: usize) -> Self {
        Self {
            filename,
            block_num,
        }
    }
}

/// The page struct that contains the contents of a page
pub struct Page {
    pub contents: Vec<u8>,
}

impl Page {
    const INT_BYTES: usize = 4;

    pub fn new(blocksize: usize) -> Self {
        Self {
            contents: vec![0; blocksize],
        }
    }

    /// Create a new page from the given bytes
    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { contents: bytes }
    }

    /// Get an integer from the page at the given offset
    fn get_int(&self, offset: usize) -> i32 {
        let bytes: [u8; Self::INT_BYTES] = self.contents[offset..offset + Self::INT_BYTES]
            .try_into()
            .unwrap();
        i32::from_be_bytes(bytes)
    }

    /// Set an integer at the given offset
    fn set_int(&mut self, offset: usize, n: i32) {
        self.contents[offset..offset + Self::INT_BYTES].copy_from_slice(&n.to_be_bytes());
    }

    /// Get a slice of bytes from the page at the given offset. Read the length and then the bytes
    fn get_bytes(&self, mut offset: usize) -> Vec<u8> {
        let bytes: [u8; Self::INT_BYTES] = self.contents[offset..offset + Self::INT_BYTES]
            .try_into()
            .unwrap();
        let length = u32::from_be_bytes(bytes) as usize;
        offset = offset + Self::INT_BYTES;
        self.contents[offset..offset + length].to_vec()
    }

    /// Set a slice of bytes at the given offset. Write the length and then the bytes
    fn set_bytes(&mut self, mut offset: usize, bytes: &[u8]) {
        let length = bytes.len() as u32;
        self.contents[offset..offset + Self::INT_BYTES].copy_from_slice(&length.to_be_bytes());
        offset = offset + Self::INT_BYTES;
        self.contents[offset..offset + bytes.len()].copy_from_slice(&bytes);
    }

    /// Get a string from the page at the given offset
    fn get_string(&self, offset: usize) -> String {
        let bytes = self.get_bytes(offset);
        String::from_utf8(bytes).unwrap()
    }

    /// Set a string at the given offset
    fn set_string(&mut self, offset: usize, string: &str) {
        self.set_bytes(offset, string.as_bytes());
    }
}

#[cfg(test)]
mod page_tests {
    use super::*;
    #[test]
    fn test_page_int_operations() {
        let mut page = Page::new(4096);
        page.set_int(100, 4000);
        assert_eq!(page.get_int(100), 4000);

        page.set_int(200, -67890);
        assert_eq!(page.get_int(200), -67890);

        page.set_int(200, 1);
        assert_eq!(page.get_int(200), 1);
    }

    #[test]
    fn test_page_string_operations() {
        let mut page = Page::new(4096);
        page.set_string(100, "Hello");
        assert_eq!(page.get_string(100), "Hello");

        page.set_string(200, "World");
        assert_eq!(page.get_string(200), "World");
    }
}

/// The file manager struct that manages the files in the database
struct FileManager {
    db_directory: PathBuf,
    blocksize: usize,
    open_files: HashMap<String, File>,
}

impl FileManager {
    fn new<P>(db_directory: &P, blocksize: usize) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let db_path = db_directory.as_ref().to_path_buf();
        fs::create_dir_all(&db_path)?;

        //  remove all existing files in the directory
        for entry in fs::read_dir(&db_path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }

        Ok(Self {
            db_directory: db_path,
            blocksize,
            open_files: HashMap::new(),
        })
    }

    /// Get the length of the file in blocks
    fn length(&mut self, filename: String) -> usize {
        let file = self.get_file(&filename);
        let len = file.metadata().unwrap().len() as usize;
        len / self.blocksize
    }

    /// Read the block provided by the block_id into the provided page
    fn read(&mut self, block_id: &BlockId, page: &mut Page) {
        let mut file = self.get_file(&block_id.filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * self.blocksize) as u64,
        ))
        .unwrap();
        match file.read_exact(&mut page.contents) {
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                page.contents = vec![0; self.blocksize];
            }
            Err(e) => panic!("Failed to read from file {}", e),
        }
    }

    /// Write the page to the block provided by the block_id
    fn write(&mut self, block_id: &BlockId, page: &mut Page) {
        let mut file = self.get_file(&block_id.filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * self.blocksize) as u64,
        ))
        .unwrap();
        file.write(&page.contents).unwrap();
    }

    /// Append a new, empty block to the file and return
    fn append(&mut self, filename: String) -> BlockId {
        let new_blk_num = self.length(filename.clone());
        let block_id = BlockId::new(filename.clone(), new_blk_num);
        let buffer = Page::new(self.blocksize);
        let mut file = self.get_file(&filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * self.blocksize).try_into().unwrap(),
        ))
        .unwrap();
        file.write(&buffer.contents).unwrap();
        block_id
    }

    /// Get the file handle for the file with the given filename
    fn get_file(&mut self, filename: &str) -> File {
        self.open_files
            .entry(filename.to_string())
            .or_insert_with(|| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(self.db_directory.join(filename))
                    .expect("Failed to open file")
            })
            .try_clone()
            .unwrap()
    }
}

#[cfg(test)]
mod file_manager_tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{test_utils::TestDir, FileManager};

    fn setup() -> (TestDir, FileManager) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let thread_id = std::thread::current().id();
        let dir = TestDir::new(format!("/tmp/test_db_{}_{:?}", timestamp, thread_id));
        let file_manger = FileManager::new(&dir, 400).unwrap();
        (dir, file_manger)
    }

    #[test]
    fn test_file_creation() {
        let (_temp_dir, mut file_manager) = setup();

        let filename = "test_file";
        file_manager.get_file(filename);

        assert!(file_manager.open_files.contains_key(filename));
    }

    #[test]
    fn test_append_and_length() {
        let (_temp_dir, mut file_manager) = setup();

        let filename = "testfile".to_string();
        assert_eq!(file_manager.length(filename.clone()), 0);

        let block_id = file_manager.append(filename.clone());
        assert_eq!(block_id.block_num, 0);
        assert_eq!(file_manager.length(filename.clone()), 1);

        let block_id_2 = file_manager.append(filename.clone());
        assert_eq!(block_id_2.block_num, 1);
        assert_eq!(file_manager.length(filename), 2);
    }
}

fn main() {
    println!("Hello, world!");
}
