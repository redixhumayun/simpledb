#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    error::Error,
    fmt::Display,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc, Condvar, Mutex, OnceLock},
    time::{Duration, Instant},
};
mod test_utils;
#[cfg(test)]
use test_utils::TestDir;

type LSN = usize;

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

trait TransactionOperations {
    fn pin(&self, block_id: &BlockId);
    fn unpin(&self, block_id: &BlockId);
    fn set_int(&self, block_id: &BlockId, offset: usize, val: i32, log: bool);
    fn set_string(&self, block_id: &BlockId, offset: usize, val: &str, log: bool);
}

impl TransactionOperations for Transaction {
    fn pin(&self, block_id: &BlockId) {
        Transaction::pin(&self, block_id);
    }

    fn unpin(&self, block_id: &BlockId) {
        Transaction::unpin(&self, block_id);
    }

    fn set_int(&self, block_id: &BlockId, offset: usize, val: i32, log: bool) {
        Transaction::set_int(&self, block_id, offset, val, log).unwrap();
    }

    fn set_string(&self, block_id: &BlockId, offset: usize, val: &str, log: bool) {
        Transaction::set_string(&self, block_id, offset, val, log).unwrap();
    }
}

type TransactionID = u64;

/// The timestamp oracle which will generate unique timestamps for each transaction
/// in a monotonically increasing fashion
struct TxIdGenerator {
    next_id: AtomicU64,
}

impl TxIdGenerator {
    fn next_id(&self) -> TransactionID {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

static TX_ID_GENERATOR: OnceLock<TxIdGenerator> = OnceLock::new();

struct Transaction {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    recovery_manager: RecoveryManager,
    concurrency_manager: ConcurrencyManager,
    buffer_list: BufferList,
    tx_id: TransactionID,
}

impl Transaction {
    const TIMEOUT: u64 = 10_000; //  10 seconds
    fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
    ) -> Self {
        let generator = TX_ID_GENERATOR.get_or_init(|| TxIdGenerator {
            next_id: AtomicU64::new(0),
        });
        let tx_id = generator.next_id();
        Self {
            tx_id,
            recovery_manager: RecoveryManager::new(
                tx_id as usize,
                Arc::clone(&log_manager),
                Arc::clone(&buffer_manager),
            ),
            buffer_list: BufferList::new(Arc::clone(&buffer_manager)),
            buffer_manager,
            log_manager,
            concurrency_manager: ConcurrencyManager::new(tx_id, Self::TIMEOUT),
            file_manager,
        }
    }

    /// Commit this transaction
    /// This will write all data associated with this transaction out to disk and append a [`LogRecord::Commit`] to the WAL
    /// It will release all locks that are currently held by this transaction
    /// It will also handle meta operations like unpinning buffers
    fn commit(&self) -> Result<(), Box<dyn Error>> {
        //  Commit all data associated with this txn
        self.recovery_manager.commit();
        //  Release all locks associated with this txn
        self.concurrency_manager.release()?;
        //  unpin all buffers and release metadata
        self.buffer_list.unpin_all();
        Ok(())
    }

    /// Rollback this transaction
    /// This will undo all operations performed by this transaction and append a [`LogRecord::Rollback`] to the WAL
    /// It will also handle meta operations like unpinning buffers
    fn rollback(&self) -> Result<(), Box<dyn Error>> {
        //  Rollback all data associated with this txn
        self.recovery_manager.rollback(self).unwrap();
        //  Release all locks associated with this txn
        self.concurrency_manager.release()?;
        //  unpin all buffers and release metadata
        self.buffer_list.unpin_all();
        Ok(())
    }

    /// Recover the database on start-up or after a crash
    fn recover(&self) -> Result<(), Box<dyn Error>> {
        //  Perform a database recovery
        self.recovery_manager.recover(self).unwrap();
        //  TODO: Release all locks associated with this transaction
        self.concurrency_manager.release()?;
        //  Unpin all buffers and release metadata
        self.buffer_list.unpin_all();
        Ok(())
    }

    /// Pin this [`BlockId`] to be used in this transaction
    fn pin(&self, block_id: &BlockId) {
        self.buffer_list.pin(block_id);
    }

    /// Unpin this [`BlockId`] since it is no longer needed by this transaction
    fn unpin(&self, block_id: &BlockId) {
        self.buffer_list.unpin(block_id);
    }

    /// Get an integer value in a [`Buffer`] associated with this transaction
    fn get_int(&self, block_id: &BlockId, offset: usize) -> Result<i32, Box<dyn Error>> {
        self.concurrency_manager.slock(block_id)?;
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let guard = buffer.lock().unwrap();
        Ok(guard.contents.get_int(offset))
    }

    /// Set an integer value in a [`Buffer`] associated with this transaction
    fn set_int(
        &self,
        block_id: &BlockId,
        offset: usize,
        value: i32,
        log: bool,
    ) -> Result<(), Box<dyn Error>> {
        self.concurrency_manager.xlock(block_id)?;
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let lsn = {
            if log {
                //  The LSN returned from writing to the WAL
                self.recovery_manager
                    .set_int(buffer.lock().unwrap().deref(), offset, value)
                    .unwrap()
            } else {
                //  The default LSN when no WAL write occurs
                LSN::MAX
            }
        };
        let mut guard = buffer.lock().unwrap();
        guard.contents.set_int(offset, value);
        guard.set_modified(self.tx_id as usize, lsn);
        Ok(())
    }

    /// Get a string value in a [`Buffer`] associated with this transaction
    fn get_string(&self, block_id: &BlockId, offset: usize) -> Result<String, Box<dyn Error>> {
        self.concurrency_manager.slock(block_id)?;
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let guard = buffer.lock().unwrap();
        Ok(guard.contents.get_string(offset))
    }

    /// Set a string value in a [`Buffer`] associated with this transaction
    fn set_string(
        &self,
        block_id: &BlockId,
        offset: usize,
        value: &str,
        log: bool,
    ) -> Result<(), Box<dyn Error>> {
        self.concurrency_manager.xlock(block_id)?;
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let lsn = {
            if log {
                self.recovery_manager
                    .set_string(buffer.lock().unwrap().deref(), offset, value)
                    .unwrap()
            } else {
                LSN::MAX
            }
        };
        let mut guard = buffer.lock().unwrap();
        guard.contents.set_string(offset, value);
        guard.set_modified(self.tx_id as usize, lsn);
        Ok(())
    }

    /// Get the available buffers for this transaction
    fn available_buffs(&self) -> usize {
        self.buffer_manager.lock().unwrap().available()
    }

    /// Get the size of this file in blocks
    fn size(&self, file_name: &str) -> usize {
        //  TODO: Insert a shared lock here to ensure the length read is accurate
        self.file_manager
            .lock()
            .unwrap()
            .length(file_name.to_string())
    }

    /// Append a block to the file
    fn append(&self, file_name: &str) -> BlockId {
        //  TODO: Insert a write lock here to ensure this write is safe
        self.file_manager
            .lock()
            .unwrap()
            .append(file_name.to_string())
    }

    /// Get the block size
    fn block_size(&self) -> usize {
        self.file_manager.lock().unwrap().blocksize
    }
}

#[cfg(test)]
mod transaction_tests {
    use std::{sync::Arc, thread::JoinHandle};

    use crate::{test_utils::generate_filename, BlockId, SimpleDB, Transaction};

    #[test]
    fn test_transaction_single_threaded() {
        let file = generate_filename();
        let block_size = 512;
        let (test_db, test_dir) = SimpleDB::new_for_test(block_size, 3);

        //  Start a transaction t1 that will set an int and a string
        let t1 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        let block_id = BlockId::new(file.to_string(), 1);
        t1.pin(&block_id);
        t1.set_int(&block_id, 80, 1, false).unwrap();
        t1.set_string(&block_id, 40, "one", false).unwrap();
        t1.commit().unwrap();

        //  Start a transaction t2 that should see the results of the previously committed transaction t1
        //  Set new values in this transaction
        let t2 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t2.pin(&block_id);
        assert_eq!(t2.get_int(&block_id, 80).unwrap(), 1);
        assert_eq!(t2.get_string(&block_id, 40).unwrap(), "one");
        t2.set_int(&block_id, 80, 2, true).unwrap();
        t2.set_string(&block_id, 40, "two", true).unwrap();
        t2.commit().unwrap();

        //  Start a transaction t3 which should see the results of t2
        //  Set new values for t3 but roll it back instead of committing
        let t3 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t3.pin(&block_id);
        assert_eq!(t3.get_int(&block_id, 80).unwrap(), 2);
        assert_eq!(t3.get_string(&block_id, 40).unwrap(), "two");
        t3.set_int(&block_id, 80, 3, true).unwrap();
        t3.set_string(&block_id, 40, "three", true).unwrap();
        t3.rollback().unwrap();

        //  Start a transaction t4 which should see the result of t2 since t3 rolled back
        //  This will be a read only transaction that commits
        let t4 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t4.pin(&block_id);
        assert_eq!(t4.get_int(&block_id, 80).unwrap(), 2);
        assert_eq!(t4.get_string(&block_id, 40).unwrap(), "two");
        t4.commit().unwrap();
    }

    #[test]
    fn test_transaction_multi_threaded_single_reader_single_writer() {
        let file = generate_filename();
        let block_size = 512;
        let (test_db, test_dir) = SimpleDB::new_for_test(block_size, 10);
        let block_id = BlockId::new(file.to_string(), 1);

        let fm1 = Arc::clone(&test_db.file_manager);
        let lm1 = Arc::clone(&test_db.log_manager);
        let bm1 = Arc::clone(&test_db.buffer_manager);
        let bid1 = block_id.clone();

        let fm2 = Arc::clone(&test_db.file_manager);
        let lm2 = Arc::clone(&test_db.log_manager);
        let bm2 = Arc::clone(&test_db.buffer_manager);
        let bid2 = block_id.clone();

        //  Create a read only transasction
        let t1 = std::thread::spawn(move || {
            let txn = Transaction::new(fm1, lm1, bm1);
            txn.pin(&bid1);
            txn.get_int(&bid1, 80).unwrap();
            txn.get_string(&bid1, 40).unwrap();
            txn.commit().unwrap();
        });

        //  Create a write only transaction
        let t2 = std::thread::spawn(move || {
            let txn = Transaction::new(fm2, lm2, bm2);
            txn.pin(&bid2.clone());
            txn.set_int(&bid2, 80, 1, false).unwrap();
            txn.set_string(&bid2, 40, "Hello", false).unwrap();
            txn.commit().unwrap();
        });
        t1.join().unwrap();
        t2.join().unwrap();

        //  Create a final read-only transaction that will read the written values
        let txn = Transaction::new(
            test_db.file_manager,
            test_db.log_manager,
            test_db.buffer_manager,
        );
        txn.pin(&block_id);
        assert_eq!(txn.get_int(&block_id, 80).unwrap(), 1);
        assert_eq!(txn.get_string(&block_id, 40).unwrap(), "Hello");
    }

    #[test]
    fn test_transaction_multi_threaded_multiple_readers_single_writer() {
        let file = generate_filename();
        let block_size = 512;
        let (test_db, test_dir) = SimpleDB::new_for_test(block_size, 10);
        let block_id = BlockId::new(file.to_string(), 1);

        let reader_threads = 10;
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        for _ in 0..reader_threads {
            let fm = Arc::clone(&test_db.file_manager);
            let lm = Arc::clone(&test_db.log_manager);
            let bm = Arc::clone(&test_db.buffer_manager);
            let bid = block_id.clone();

            handles.push(std::thread::spawn(move || {
                let txn = Transaction::new(fm, lm, bm);
                txn.pin(&bid);
                txn.get_int(&bid, 80).unwrap();
                txn.get_string(&bid, 40).unwrap();
                txn.commit().unwrap();
            }));
        }

        let txn = Transaction::new(
            test_db.file_manager.clone(),
            test_db.log_manager.clone(),
            test_db.buffer_manager.clone(),
        );
        txn.pin(&block_id);
        txn.set_int(&block_id, 80, 1, false).unwrap();
        txn.set_string(&block_id, 40, "Hello", false).unwrap();
        txn.commit().unwrap();

        handles
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
    }
}

struct LockState {
    readers: HashSet<TransactionID>, //  keep track of which transaction id's have a reader lock here
    writer: Option<TransactionID>,   //  keep track of the transaction writing to a specific block
    upgrade_request: Option<TransactionID>, //  keep track of upgrade requests to prevent writer starvation
}

/// Global struct used by all transactions to keep track of locks
struct LockTable {
    lock_table: Mutex<HashMap<BlockId, LockState>>,
    cond_var: Condvar,
    timeout: u64,
}

impl LockTable {
    fn new(timeout: u64) -> Self {
        Self {
            lock_table: Mutex::new(HashMap::new()),
            cond_var: Condvar::new(),
            timeout,
        }
    }

    /// Acquire a shared lock on a [`BlockId`] for a [`Transaction`]
    fn acquire_shared_lock(
        &self,
        tx_id: TransactionID,
        block_id: &BlockId,
    ) -> Result<(), Box<dyn Error>> {
        let mut lock_table_guard = self.lock_table.lock().unwrap();
        lock_table_guard
            .entry(block_id.clone())
            .or_insert(LockState {
                readers: vec![tx_id].into_iter().collect(),
                writer: None,
                upgrade_request: None,
            });

        //  Do an early return if the txn already has an SLock on this block
        if lock_table_guard
            .get(block_id)
            .unwrap()
            .readers
            .contains(&tx_id)
        {
            return Ok(());
        }

        //  Loop until either
        //  1. There are no more writers or pending writers on this block
        //  2. The timeout expires
        let deadline = Instant::now() + Duration::from_millis(self.timeout);
        loop {
            let state = lock_table_guard.get_mut(block_id).unwrap();
            let should_wait = state.writer.is_some() || state.upgrade_request.is_some();

            if !should_wait {
                break;
            }

            lock_table_guard = self.cond_var.wait(lock_table_guard).unwrap();

            if Instant::now() >= deadline {
                return Err("Timeout while waiting for lock".into());
            }
        }
        lock_table_guard
            .get_mut(block_id)
            .unwrap()
            .readers
            .insert(tx_id);
        Ok(())
    }

    /// Acquire an exclusive lock on a [`BlockId`] for a [`Transaction`]
    fn acquire_write_lock(
        &self,
        tx_id: TransactionID,
        block_id: &BlockId,
    ) -> Result<(), Box<dyn Error>> {
        let mut lock_table_guard = self.lock_table.lock().unwrap();
        lock_table_guard
            .entry(block_id.clone())
            .or_insert(LockState {
                readers: HashSet::from_iter(vec![tx_id]),
                writer: Some(tx_id),
                upgrade_request: None,
            });

        //  Do an early return if this txn already has an xlock on the buffer
        if lock_table_guard
            .get(block_id)
            .unwrap()
            .writer
            .map_or(false, |id| id == tx_id)
        {
            return Ok(());
        }

        //  Maintain the invariant that any transaction that wants an xlock must first have an slock
        assert!(lock_table_guard
            .get(block_id)
            .unwrap()
            .readers
            .contains(&tx_id), "Transaction {} failed to have an slock before attempting to acquire xlock on block id {:?}", tx_id, block_id);

        let is_upgrade = lock_table_guard
            .get(block_id)
            .unwrap()
            .readers
            .contains(&tx_id);
        if is_upgrade {
            if lock_table_guard
                .get(block_id)
                .unwrap()
                .upgrade_request
                .is_some()
            {
                return Err("Upgrade request already exists".into());
            }
            lock_table_guard.get_mut(block_id).unwrap().upgrade_request = Some(tx_id);
            let deadline = Instant::now() + Duration::from_millis(self.timeout);
            loop {
                let state = lock_table_guard.get_mut(block_id).unwrap();
                let should_wait = state.readers.len() > 1 || state.writer.is_some();

                if !should_wait {
                    break;
                }

                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    return Err("Timeout while waiting for lock".into());
                }
                let (guard, timeout_reached) = self
                    .cond_var
                    .wait_timeout(lock_table_guard, timeout)
                    .unwrap();
                lock_table_guard = guard;

                if timeout_reached.timed_out() {
                    return Err("Timeout while waiting for lock".into());
                }

                if Instant::now() > deadline {
                    return Err("Timeout while waiting for lock".into());
                }
            }
            let state = lock_table_guard.get_mut(block_id).unwrap();
            assert_eq!(state.readers.len(), 1);
            assert_eq!(state.readers.contains(&tx_id), true);
            state.readers.remove(&tx_id);
            state.writer = Some(tx_id);
            state.upgrade_request = None;
            return Ok(());
        } else {
            let deadline = Instant::now() + Duration::from_millis(self.timeout);
            loop {
                let state = lock_table_guard.get_mut(block_id).unwrap();
                let should_wait = state.readers.len() > 0 || state.writer.is_some();

                if !should_wait {
                    break;
                }

                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    return Err("Timeout while waiting for lock".into());
                }
                let (guard, timeout_reached) = self
                    .cond_var
                    .wait_timeout(lock_table_guard, timeout)
                    .unwrap();
                lock_table_guard = guard;

                if timeout_reached.timed_out() {
                    return Err("Timeout while waiting for lock".into());
                }

                if Instant::now() > deadline {
                    return Err("Timeout while waiting for lock".into());
                }
            }
            lock_table_guard.get_mut(block_id).unwrap().writer = Some(tx_id);
            return Ok(());
        }
    }

    /// Release all locks on a specific [`BlockId`] that were acquired by a [`Transaction`]
    fn release_locks(
        &self,
        tx_id: TransactionID,
        block_id: &BlockId,
    ) -> Result<(), Box<dyn Error>> {
        let mut lock_table_guard = self.lock_table.lock().unwrap();
        if let Some(state) = lock_table_guard.get_mut(block_id) {
            state.readers.remove(&tx_id);
            if let Some(writer_tx_id) = state.writer {
                if writer_tx_id == tx_id {
                    state.writer = None;
                }
            }
            if let Some(upgrade_req_tx_id) = state.upgrade_request {
                if upgrade_req_tx_id == tx_id {
                    state.upgrade_request = None;
                }
            }
        }
        self.cond_var.notify_all();
        return Ok(());
    }
}

#[cfg(test)]
mod lock_table_tests {
    use std::{sync::Arc, time::Duration};

    use crate::{test_utils::generate_filename, BlockId, LockTable};

    #[test]
    fn test_basic_shared_lock() {
        let filename = generate_filename();
        let lock_table = Arc::new(LockTable::new(10_000));
        let block_id = BlockId::new(filename, 1);

        // Should be able to acquire shared lock
        lock_table.acquire_shared_lock(1, &block_id).unwrap();

        // Another transaction should also be able to acquire shared lock
        lock_table.acquire_shared_lock(2, &block_id).unwrap();

        // Release locks
        lock_table.release_locks(1, &block_id).unwrap();
        lock_table.release_locks(2, &block_id).unwrap();
    }

    #[test]
    fn test_basic_exclusive_lock() {
        let filename = generate_filename();
        let lock_table = Arc::new(LockTable::new(1)); //  extremely short timeout of 1ms
        let block_id = BlockId::new(filename, 1);

        // Should be able to acquire exclusive lock
        lock_table.acquire_write_lock(1, &block_id).unwrap();

        let lt_1 = Arc::clone(&lock_table);
        let bid_1 = block_id.clone();

        //  Another transaction should not be able to acquire any locks
        let handle = std::thread::spawn(move || {
            lt_1.acquire_shared_lock(2, &bid_1).unwrap_err();
        });

        // Release lock after a timeout of making sure t2 panics
        std::thread::sleep(Duration::from_millis(5));
        lock_table.release_locks(1, &block_id).unwrap();
    }

    #[test]
    fn test_read_write_interleaving() {
        let lock_table = Arc::new(LockTable::new(1000)); //  timeout of 1sec
        let block_id = BlockId::new(generate_filename(), 1);

        //  reader thread
        let lt_1 = Arc::clone(&lock_table);
        let bid_1 = block_id.clone();
        std::thread::spawn(move || {
            let readers = 10;
            for i in 0..readers {
                lt_1.acquire_shared_lock(i, &bid_1).unwrap();
                std::thread::sleep(Duration::from_millis(100));
                lt_1.release_locks(i, &bid_1).unwrap();
            }
        });

        //  writer thread
        let lt_2 = Arc::clone(&lock_table);
        let bid_2 = block_id.clone();
        std::thread::spawn(move || {
            let count = 10;
            let mut iterations = 0;
            loop {
                if iterations == count {
                    break;
                }

                lt_2.acquire_shared_lock(12, &bid_2).unwrap();
                lt_2.acquire_write_lock(12, &bid_2).unwrap();
                lt_2.release_locks(12, &bid_2).unwrap();

                iterations += 1;
            }
        });
    }

    #[test]
    fn test_lock_upgrade() {
        let lock_table = Arc::new(LockTable::new(1000));
        let block_id = BlockId::new(generate_filename(), 1);
        let (tx, rx) = std::sync::mpsc::channel::<String>();

        //  T1 acquires shared lock
        lock_table.acquire_shared_lock(1, &block_id).unwrap();

        //  T2 acquires shared lock
        lock_table.acquire_shared_lock(2, &block_id).unwrap();

        //  T1 requests an upgrade
        let lt1 = Arc::clone(&lock_table);
        let bid1 = block_id.clone();
        let j1 = std::thread::spawn(move || {
            tx.send("Acquiring write lock".to_string()).unwrap();
            lt1.acquire_write_lock(1, &bid1).unwrap();
            tx.send("Acquired write lock".to_string()).unwrap();
        });

        //  Wait for T1 to start acquiring write lock and release T2's lock
        assert!(rx.recv().unwrap() == "Acquiring write lock".to_string());
        lock_table.release_locks(2, &block_id).unwrap();
        assert!(rx.recv().unwrap() == "Acquired write lock".to_string());
    }
}

/// The static instance of the lock table
static LOCK_TABLE_GENERATOR: OnceLock<Arc<LockTable>> = OnceLock::new();

enum LockType {
    Shared,
    Exclusive,
}

struct ConcurrencyManager {
    lock_table: Arc<LockTable>,
    locks: RefCell<HashMap<BlockId, LockType>>,
    tx_id: TransactionID,
}

impl ConcurrencyManager {
    fn new(tx_id: TransactionID, timeout: u64) -> Self {
        Self {
            lock_table: LOCK_TABLE_GENERATOR
                .get_or_init(|| Arc::new(LockTable::new(timeout)))
                .clone(),
            locks: RefCell::new(HashMap::new()),
            tx_id,
        }
    }

    /// Acquire a shared lock on a [`BlockId`] for the associated [`Transaction`]
    fn slock(&self, block_id: &BlockId) -> Result<(), Box<dyn Error>> {
        let mut locks = self.locks.borrow_mut();
        if locks.contains_key(block_id) {
            return Ok(());
        }
        self.lock_table.acquire_shared_lock(self.tx_id, block_id)?;
        locks.insert(block_id.clone(), LockType::Shared);
        Ok(())
    }

    /// Acquire an exclusive lock on a [`BlockId`] for the associated [`Transaction`]
    /// It will first check to see if there is already a [`LockType`] available on the [`BlockId`]
    /// If there is none, it will first attempt to acquire a [`LockType::Shared`] and then a [`LockType::Exclusive`]
    fn xlock(&self, block_id: &BlockId) -> Result<(), Box<dyn Error>> {
        let mut locks = self.locks.borrow_mut();
        match locks.get(block_id) {
            Some(lock) => match lock {
                LockType::Shared => {
                    self.lock_table.acquire_write_lock(self.tx_id, block_id)?;
                    locks.insert(block_id.clone(), LockType::Exclusive).unwrap();
                }
                LockType::Exclusive => return Ok(()),
            },
            None => {
                //  drop the value here so no overlapping borrows
                drop(locks);
                self.slock(block_id)?;
                self.lock_table.acquire_write_lock(self.tx_id, block_id)?;

                //  re-acquire the borrow mut here
                let mut locks = self.locks.borrow_mut();
                locks.insert(block_id.clone(), LockType::Exclusive);
            }
        }
        return Ok(());
    }

    /// Release all locks associated with a [`Transaction`]
    fn release(&self) -> Result<(), Box<dyn Error>> {
        let mut locks = self.locks.borrow_mut();
        for block in locks.keys() {
            self.lock_table.release_locks(self.tx_id, block)?;
        }
        locks.clear();
        Ok(())
    }
}

/// The container for the recovery manager - a [`Transaction`] uses a unique instance of this to
/// manage writing records to WAL and handling recovery & rollback
struct RecoveryManager {
    tx_num: usize,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl RecoveryManager {
    fn new(
        tx_num: usize,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
    ) -> Self {
        Self {
            tx_num,
            log_manager,
            buffer_manager,
        }
    }

    /// Commit the [`Transaction`]
    /// It flushes all the buffers associated with this transaction
    /// It creates and writes a new [`LogRecord::Commit`] record to the WAL
    /// It then forces a flush on the WAL to ensure logs are committed
    fn commit(&self) {
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
        let record = LogRecord::Commit(self.tx_num);
        let lsn = record
            .write_log_record(Arc::clone(&self.log_manager))
            .unwrap();
        self.log_manager.lock().unwrap().flush_lsn(lsn);
    }

    /// Rollback the [`Transaction`] associated with this [`RecoveryManager`] instance
    /// Iterate over the WAL records in reverse order and undo any modifications done for this [`Transaction`]
    /// Flush all data associated with this transaction
    /// Create, write and flush a [`LogRecord::Checkpoint`] record
    fn rollback(&self, tx: &dyn TransactionOperations) -> Result<(), Box<dyn Error>> {
        //  Perform the actual rollback by reading the files from WAL and undoing all changes made by this txn
        let log_iter = self.log_manager.lock().unwrap().iterator();
        for log in log_iter {
            let record = LogRecord::from_bytes(log)?;
            if record.get_tx_num() != self.tx_num {
                continue;
            }
            if let LogRecord::Start(_) = record {
                return Ok(());
            }
            record.undo(tx);
        }
        //  Flush all data associated with this transaction
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
        //  Write a checkpoint record and flush it
        let checkpoint_record = LogRecord::Checkpoint;
        let lsn = checkpoint_record.write_log_record(Arc::clone(&self.log_manager))?;
        self.log_manager.lock().unwrap().flush_lsn(lsn);
        Ok(())
    }

    /// Recover the database from the last [`LogRecord::Checkpoint`]
    /// Find all the incomplete transactions and undo their operations
    /// Write a quiescent [`LogRecord::Checkpoint`] to the log and flush it
    fn recover(&self, tx: &dyn TransactionOperations) -> Result<(), Box<dyn Error>> {
        //  Iterate over the WAL records in reverse order and add any that don't have a COMMIT to unfinished txns
        let log_iter = self.log_manager.lock().unwrap().iterator();
        let mut finished_txns: Vec<usize> = Vec::new();
        for log in log_iter {
            let record = LogRecord::from_bytes(log)?;
            match record {
                LogRecord::Checkpoint => return Ok(()),
                LogRecord::Commit(_) | LogRecord::Rollback(_) => {
                    finished_txns.push(record.get_tx_num());
                }
                _ => {
                    if !finished_txns.contains(&record.get_tx_num()) {
                        record.undo(tx);
                    }
                }
            }
        }
        //  Flush all data associated with this transaction
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
        //  Write a checkpoint record and flush it
        let checkpoint_record = LogRecord::Checkpoint;
        let lsn = checkpoint_record.write_log_record(Arc::clone(&self.log_manager))?;
        self.log_manager.lock().unwrap().flush_lsn(lsn);
        Ok(())
    }

    /// Write the [`LogRecord`] to set the value of an integer in a [`Buffer`]
    fn set_int(
        &self,
        buffer: &Buffer,
        offset: usize,
        _new_value: i32,
    ) -> Result<LSN, Box<dyn Error>> {
        let old_value = buffer.contents.get_int(offset);
        let block_id = buffer.block_id.clone().unwrap();
        let record = LogRecord::SetInt {
            txnum: self.tx_num,
            block_id,
            offset,
            old_val: old_value,
        };
        record.write_log_record(Arc::clone(&self.log_manager))
    }

    /// Write the [`LogRecord`] to set the value of a String in a [`Buffer`]
    fn set_string(
        &self,
        buffer: &Buffer,
        offset: usize,
        _new_value: &str,
    ) -> Result<LSN, Box<dyn Error>> {
        let old_value = buffer.contents.get_string(offset);
        let block_id = buffer.block_id.clone().unwrap();
        let record = LogRecord::SetString {
            txnum: self.tx_num,
            block_id,
            offset,
            old_val: old_value,
        };
        record.write_log_record(Arc::clone(&self.log_manager))
    }
}

#[cfg(test)]
mod recovery_manager_tests {
    use std::sync::{Arc, Mutex};

    use crate::{BlockId, LogRecord, RecoveryManager, SimpleDB, TransactionOperations};

    struct MockTransaction {
        pinned_blocks: Vec<BlockId>,
        modified_ints: Mutex<Vec<(BlockId, usize, i32)>>,
        modified_strings: Mutex<Vec<(BlockId, usize, String)>>,
    }

    impl MockTransaction {
        fn new() -> Self {
            Self {
                pinned_blocks: Vec::new(),
                modified_ints: Mutex::new(Vec::new()),
                modified_strings: Mutex::new(Vec::new()),
            }
        }

        fn verify_int_was_reset(
            &self,
            block_id: &BlockId,
            offset: usize,
            expected_val: i32,
        ) -> bool {
            self.modified_ints
                .lock()
                .unwrap()
                .iter()
                .any(|(b, o, v)| b == block_id && *o == offset && *v == expected_val)
        }

        fn verify_string_was_reset(
            &self,
            block_id: &BlockId,
            offset: usize,
            expected_val: String,
        ) -> bool {
            self.modified_strings
                .lock()
                .unwrap()
                .iter()
                .any(|(b, o, v)| b == block_id && *o == offset && *v == expected_val)
        }
    }

    impl TransactionOperations for MockTransaction {
        fn pin(&self, block_id: &BlockId) {
            dbg!("Pinning block {:?}", block_id);
        }

        fn unpin(&self, block_id: &BlockId) {
            dbg!("Unpinning block {:?}", block_id);
        }

        fn set_int(&self, block_id: &BlockId, offset: usize, val: i32, log: bool) {
            dbg!(
                "Setting int at block {:?} offset {} to {}",
                block_id,
                offset,
                val
            );
            self.modified_ints
                .lock()
                .unwrap()
                .push((block_id.clone(), offset, val));
        }

        fn set_string(&self, block_id: &BlockId, offset: usize, val: &str, log: bool) {
            dbg!(
                "Setting string at block {:?} offset {} to {}",
                block_id,
                offset,
                val
            );
            self.modified_strings
                .lock()
                .unwrap()
                .push((block_id.clone(), offset, val.to_string()));
        }
    }

    #[test]
    fn test_rollback_with_int() {
        let (db, _test_dir) = SimpleDB::new_for_test(400, 3);

        let recovery_manager = RecoveryManager::new(
            1,
            Arc::clone(&db.log_manager),
            Arc::clone(&db.buffer_manager),
        );

        let mut mock_tx = MockTransaction::new();
        let test_block = BlockId::new("test.txt".to_string(), 1);

        // Write some log records that will need to be rolled back
        let set_int_record = LogRecord::SetInt {
            txnum: 1,
            block_id: test_block.clone(),
            offset: 0,
            old_val: 100, // Original value before modification
        };
        set_int_record
            .write_log_record(Arc::clone(&db.log_manager))
            .unwrap();

        // Perform rollback
        recovery_manager.rollback(&mut mock_tx).unwrap();

        // Verify that the value was reset to the original value
        assert!(mock_tx.verify_int_was_reset(&test_block, 0, 100));
        assert_eq!(
            mock_tx.modified_ints.lock().unwrap().len(),
            1,
            "Should have exactly one modification"
        );
    }

    #[test]
    fn test_rollback_with_string() {
        let (db, _test_dir) = SimpleDB::new_for_test(400, 3);
        let recovery_manager = RecoveryManager::new(
            1,
            Arc::clone(&db.log_manager),
            Arc::clone(&db.buffer_manager),
        );

        let mut mock_tx = MockTransaction::new();
        let test_block = BlockId::new("test.txt".to_string(), 1);

        //  Write some log records that will need to be rolled back
        let set_string_record = LogRecord::SetString {
            txnum: 1,
            block_id: test_block.clone(),
            offset: 0,
            old_val: "Hello World".to_string(),
        };
        set_string_record
            .write_log_record(Arc::clone(&db.log_manager))
            .unwrap();

        //   Perform rollback
        recovery_manager.rollback(&mut mock_tx).unwrap();

        //  Verify that the value was reset to the original value
        assert!(mock_tx.verify_string_was_reset(&test_block, 0, "Hello World".to_string()));
        assert_eq!(
            mock_tx.modified_strings.lock().unwrap().len(),
            1,
            "Should have exactly one modification"
        );
    }
}

/// The container for all the different types of log records that are written to the WAL
#[derive(Clone)]
enum LogRecord {
    Start(usize),
    Commit(usize),
    Rollback(usize),
    Checkpoint,
    SetInt {
        txnum: usize,
        block_id: BlockId,
        offset: usize,
        old_val: i32,
    },
    SetString {
        txnum: usize,
        block_id: BlockId,
        offset: usize,
        old_val: String,
    },
}

impl Display for LogRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogRecord::Start(txnum) => write!(f, "Start({})", txnum),
            LogRecord::Commit(txnum) => write!(f, "Commit({})", txnum),
            LogRecord::Rollback(txnum) => write!(f, "Rollback({})", txnum),
            LogRecord::Checkpoint => write!(f, "Checkpoint"),
            LogRecord::SetInt {
                txnum,
                block_id,
                offset,
                old_val,
            } => write!(
                f,
                "SetInt(txnum: {}, block_id: {:?}, offset: {}, old_val: {})",
                txnum, block_id, offset, old_val
            ),
            LogRecord::SetString {
                txnum,
                block_id,
                offset,
                old_val,
            } => write!(
                f,
                "SetString(txnum: {}, block_id: {:?}, offset: {}, old_val: {})",
                txnum, block_id, offset, old_val
            ),
        }
    }
}

impl TryInto<Vec<u8>> for &LogRecord {
    type Error = Box<dyn Error>;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        let size = self.calculate_size();
        let int_value = self.discriminant();
        let mut page = Page::new(size);
        let mut pos = 0;
        page.set_int(pos, int_value as i32);
        pos += 4;
        match self {
            LogRecord::Start(txnum) => {
                page.set_int(pos, *txnum as i32);
            }
            LogRecord::Commit(txnum) => {
                page.set_int(pos, *txnum as i32);
            }
            LogRecord::Rollback(txnum) => {
                page.set_int(pos, *txnum as i32);
            }
            LogRecord::Checkpoint => {}
            LogRecord::SetInt {
                txnum,
                block_id,
                offset,
                old_val,
            } => {
                page.set_int(pos, *txnum as i32);
                pos += 4;
                page.set_string(pos, &block_id.filename);
                pos += 4 + block_id.filename.len();
                page.set_int(pos, block_id.block_num as i32);
                pos += 4;
                page.set_int(pos, *offset as i32);
                pos += 4;
                page.set_int(pos, *old_val);
            }
            LogRecord::SetString {
                txnum,
                block_id,
                offset,
                old_val,
            } => {
                page.set_int(pos, *txnum as i32);
                pos += 4;
                page.set_string(pos, &block_id.filename);
                pos += 4 + block_id.filename.len();
                page.set_int(pos, block_id.block_num as i32);
                pos += 4;
                page.set_int(pos, *offset as i32);
                pos += 4;
                page.set_string(pos, old_val);
            }
        }
        Ok(page.contents)
    }
}

impl TryFrom<Vec<u8>> for LogRecord {
    type Error = Box<dyn Error>;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let page = Page::from_bytes(value);
        let mut pos = 0;
        let discriminant = page.get_int(pos);
        pos += 4;

        match discriminant {
            0 => Ok(LogRecord::Start(page.get_int(pos) as usize)),
            1 => Ok(LogRecord::Commit(page.get_int(pos) as usize)),
            2 => Ok(LogRecord::Rollback(page.get_int(pos) as usize)),
            3 => Ok(LogRecord::Checkpoint),
            4 => {
                let txnum = page.get_int(pos) as usize;
                pos += 4;
                let filename = page.get_string(pos);
                pos += 4 + filename.len();
                let block_num = page.get_int(pos) as usize;
                pos += 4;
                let offset = page.get_int(pos) as usize;
                pos += 4;
                let old_val = page.get_int(pos);

                return Ok(LogRecord::SetInt {
                    txnum,
                    block_id: BlockId::new(filename, block_num),
                    offset,
                    old_val,
                });
            }
            5 => {
                let txnum = page.get_int(pos) as usize;
                pos += 4;
                let filename = page.get_string(pos);
                pos += 4 + filename.len();
                let block_num = page.get_int(pos) as usize;
                pos += 4;
                let offset = page.get_int(pos) as usize;
                pos += 4;
                let old_val = page.get_string(pos);

                return Ok(LogRecord::SetString {
                    txnum,
                    block_id: BlockId::new(filename, block_num),
                    offset,
                    old_val,
                });
            }
            _ => Err("Invalid log record type".into()),
        }
    }
}

impl LogRecord {
    // Size constants for different components
    const DISCRIMINANT_SIZE: usize = Page::INT_BYTES;
    const TXNUM_SIZE: usize = Page::INT_BYTES;
    const OFFSET_SIZE: usize = Page::INT_BYTES;
    const BLOCK_NUM_SIZE: usize = Page::INT_BYTES;
    const STR_LEN_SIZE: usize = Page::INT_BYTES;

    fn calculate_size(&self) -> usize {
        let base_size = Self::DISCRIMINANT_SIZE; // Every record has a discriminant
        match self {
            LogRecord::Start(_) | LogRecord::Commit(_) | LogRecord::Rollback(_) => {
                base_size + Self::TXNUM_SIZE
            }
            LogRecord::Checkpoint => base_size,
            LogRecord::SetInt { block_id, .. } => {
                base_size
                    + Self::TXNUM_SIZE
                    + Self::STR_LEN_SIZE
                    + block_id.filename.len()
                    + Self::BLOCK_NUM_SIZE
                    + Self::OFFSET_SIZE
                    + Self::TXNUM_SIZE // NOTE: old_val size (be careful of this changing)
            }
            LogRecord::SetString {
                block_id, old_val, ..
            } => {
                base_size
                    + Self::TXNUM_SIZE
                    + Self::STR_LEN_SIZE
                    + block_id.filename.len()
                    + Self::BLOCK_NUM_SIZE
                    + Self::OFFSET_SIZE
                    + Self::STR_LEN_SIZE
                    + old_val.len()
            }
        }
    }

    /// Get the discriminant value for the log record
    fn discriminant(&self) -> u32 {
        match self {
            LogRecord::Start(_) => 0,
            LogRecord::Commit(_) => 1,
            LogRecord::Rollback(_) => 2,
            LogRecord::Checkpoint => 3,
            LogRecord::SetInt { .. } => 4,
            LogRecord::SetString { .. } => 5,
        }
    }

    /// Get the transaction number associated with this log record
    /// Will panic for certain log records
    fn get_tx_num(&self) -> usize {
        match self {
            LogRecord::Start(txnum) => *txnum,
            LogRecord::Commit(txnum) => *txnum,
            LogRecord::Checkpoint => panic!("Checkpoint does not have a transaction number"),
            LogRecord::Rollback(txnum) => *txnum,
            LogRecord::SetInt { txnum, .. } => *txnum,
            LogRecord::SetString { txnum, .. } => *txnum,
        }
    }

    /// Undo the operation performed by this log record
    /// This is used by the [`RecoveryManager`] when performing a recovery
    fn undo(&self, tx: &dyn TransactionOperations) {
        match self {
            LogRecord::Start(_) => (),    //  no-op
            LogRecord::Commit(_) => (),   //  no-op
            LogRecord::Rollback(_) => (), //  no-op
            LogRecord::Checkpoint => (),  //  no-op
            LogRecord::SetInt {
                block_id,
                offset,
                old_val,
                ..
            } => {
                tx.pin(block_id);
                tx.set_int(block_id, *offset, *old_val, false);
                tx.unpin(block_id);
            }
            LogRecord::SetString {
                block_id,
                offset,
                old_val,
                ..
            } => {
                tx.pin(block_id);
                tx.set_string(block_id, *offset, old_val, false);
                tx.unpin(block_id);
            }
        }
    }

    /// Serialize the log record to bytes and write it to the log file
    fn write_log_record(&self, log_manager: Arc<Mutex<LogManager>>) -> Result<LSN, Box<dyn Error>> {
        let bytes: Vec<u8> = self.try_into()?;
        Ok(log_manager.lock().unwrap().append(bytes))
    }

    /// Read the bytes from the log file and deserialize them into a [`LogRecord`]
    fn from_bytes(bytes: Vec<u8>) -> Result<LogRecord, Box<dyn Error>> {
        let result: LogRecord = bytes.try_into()?;
        Ok(result)
    }
}

/// Wrapper for the value contained in the hash map of the [`BufferList`]
struct HashMapValue {
    buffer: Arc<Mutex<Buffer>>,
    count: usize,
}

/// A wrapper to maintain the list of [`Buffer`] being used by the [`Transaction`]
/// It uses the [`BufferManager`] internally to maintain metadata
struct BufferList {
    buffers: RefCell<HashMap<BlockId, HashMapValue>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl BufferList {
    fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> Self {
        Self {
            buffers: RefCell::new(HashMap::new()),
            buffer_manager,
        }
    }

    /// Get the buffer associated with the provided block_id
    fn get_buffer(&self, block_id: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        self.buffers
            .borrow()
            .get(block_id)
            .and_then(|v| Some(Arc::clone(&v.buffer)))
    }

    /// Pin the buffer associated with the provided [`BlockId`]
    fn pin(&self, block_id: &BlockId) {
        let buffer = self.buffer_manager.lock().unwrap().pin(block_id).unwrap();
        self.buffers
            .borrow_mut()
            .entry(block_id.clone())
            .and_modify(|v| v.count += 1)
            .or_insert(HashMapValue { buffer, count: 1 });
    }

    /// Unpin the buffer associated with the provided [`BlockId`]
    fn unpin(&self, block_id: &BlockId) {
        assert!(self.buffers.borrow().contains_key(block_id));
        let buffer = Arc::clone(&self.buffers.borrow().get(block_id).unwrap().buffer);
        self.buffer_manager.lock().unwrap().unpin(buffer);
        let should_remove = {
            let mut buffers = self.buffers.borrow_mut();
            let v = buffers.get_mut(block_id).unwrap();
            v.count -= 1;
            v.count == 0
        };
        if should_remove {
            self.buffers.borrow_mut().remove(block_id);
        }
    }

    /// Unpin all the buffers in this [`BufferList`]
    fn unpin_all(&self) {
        let mut buffer_guard = self.buffers.borrow_mut();
        let buffers = buffer_guard.values();
        for buffer in buffers {
            self.buffer_manager
                .lock()
                .unwrap()
                .unpin(Arc::clone(&buffer.buffer));
        }
        buffer_guard.clear();
    }
}

#[cfg(test)]
mod buffer_list_tests {
    use std::sync::{Arc, Mutex};

    use crate::{test_utils::TestDir, BlockId, BufferList, BufferManager, FileManager, LogManager};

    #[test]
    fn test_buffer_list_functionality() {
        let dir = TestDir::new("buffer_list_tests");
        let file_manager = Arc::new(Mutex::new(FileManager::new(&dir, 400).unwrap()));
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&file_manager),
            "buffer_list_tests_log_file",
        )));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(file_manager, log_manager, 4)));
        let buffer_list = BufferList::new(buffer_manager);

        //  check that there are no buffers in the buffer list initially
        let block_id = BlockId {
            filename: "testfile".to_string(),
            block_num: 1,
        };
        assert!(buffer_list.get_buffer(&block_id).is_none());

        //  pinning a buffer and then attempting to fetch it should return the correct one
        buffer_list.pin(&block_id);
        assert!(buffer_list.get_buffer(&block_id).is_some());

        //  unpinning all buffers will empty the buffer list
        buffer_list.unpin_all();
        assert!(buffer_list.buffers.borrow().is_empty());
    }
}

struct Buffer {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    contents: Page,
    block_id: Option<BlockId>,
    pub pins: usize,
    txn: Option<usize>,
    lsn: Option<LSN>,
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
    /// It depends on [`BufferManager::try_to_pin`] to get a buffer back
    /// Once the buffer has been retrieved, it will handle metadata operations
    fn pin(&self, block_id: &BlockId) -> Result<Arc<Mutex<Buffer>>, Box<dyn Error>> {
        let start = Instant::now();
        let mut num_available = self.num_available.lock().unwrap();
        loop {
            match self.try_to_pin(block_id) {
                Some(buffer) => {
                    {
                        let mut buffer_guard = buffer.lock().unwrap();
                        if !buffer_guard.is_pinned() {
                            *num_available -= 1;
                        }
                        buffer_guard.pin();
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
        dbg!("creating records");
        for i in start..=end {
            let record = create_log_record(&format!("record{i}"), i + 100);
            let lsn = log_manager.lock().unwrap().append(record);
            print!("{lsn} ");
        }
        dbg!("");
    }

    fn print_log_records(log_manager: Arc<Mutex<LogManager>>, message: &str) {
        dbg!("{message}");
        let iter = log_manager.lock().unwrap().iterator();

        for record in iter {
            let length = i32::from_be_bytes(record[..4].try_into().unwrap());
            let string = String::from_utf8(record[4..4 + length as usize].to_vec()).unwrap();
            let n = usize::from_be_bytes(record[4 + (length as usize)..].try_into().unwrap());
            dbg!("{string} {n}");
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
    fn flush_lsn(&mut self, lsn: LSN) {
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
    fn append(&mut self, log_record: Vec<u8>) -> LSN {
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
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
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

fn main() {}
