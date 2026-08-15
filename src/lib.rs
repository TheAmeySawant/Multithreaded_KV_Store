// Inbuild libs
use prost::Message;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, create_dir_all},
    io::{
        ErrorKind::{self, UnexpectedEof},
        Read, Write,
    },
    path::Path,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
    vec,
};

// Windows Specific imports
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;

#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
};

// External libs
use crc32fast::Hasher;
use tokio::{
    sync::{Notify, RwLock},
    time::interval,
};

//Protocol Buffer Data Structures
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/kvstore.rs"));
}
pub use proto::{DeleteKv, ReadKv, Wal, WriteKv, available_operations_on_kv::Op};

use proto::AvailableOperationsOnKv;

use crate::proto::Snapshot;

type TokioMutex<T> = tokio::sync::Mutex<T>;

#[derive(Clone)]
pub struct AppState {
    stores_map: StoresMap,
    /// true -> Working/On , false -> Stopped/Off
    access_tracker_flag: Arc<AtomicBool>,
}

impl AppState {
    pub fn new() -> AppState {
        AppState {
            stores_map: StoresMap::new(RwLock::new(Stores::new())),
            access_tracker_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn get_stores_map_clone(&self) -> StoresMap {
        Arc::clone(&self.stores_map)
    }

    /// access_tracker stops
    pub fn stop_access_tracker(&self) {
        self.access_tracker_flag.store(false, Ordering::Release);
    }

    /// Spawns a looping task which resets current_access_count and calculates avg_access_count after each *interval_in_seconds*
    pub fn spawn_access_tracker(&self, interval_in_seconds: Option<u64>) {
        let interval_in_seconds = match interval_in_seconds {
            Some(secs) => secs,
            None => 120, //Default 2 minutes
        };

        // set access_tracker_flag = true
        self.access_tracker_flag.store(true, Ordering::Release);

        let stores_map = Arc::clone(&self.stores_map);
        let access_tracker_flag = Arc::clone(&self.access_tracker_flag);

        tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(interval_in_seconds));

            loop {
                // Check Access Tracker Flag
                if !(access_tracker_flag.load(Ordering::Acquire)) {
                    break;
                }

                timer.tick().await;

                // Check Access Tracker Flag
                if !(access_tracker_flag.load(Ordering::Acquire)) {
                    break;
                }

                let mut stores_map_guard = stores_map.write().await;

                stores_map_guard.update_access_tracker();
            }
        });
    }

    pub async fn graceful_shutdown(&self) -> Result<(), String> {
        self.stop_access_tracker();

        let stores = {
            let stores_map_guard = self.stores_map.read().await;

            stores_map_guard.inner.values().cloned().collect::<Vec<_>>()
        };

        for store_engine in stores {
            store_engine.graceful_shutdown().await?;
        }

        Ok(())
    }
}

/// ### MemStorage
/// In-Memory Storage of KV
#[derive(Debug)]
struct MemStorage {
    sq_no: u64, //Sequence Number
    mem_storage: HashMap<String, String>,
}

impl MemStorage {
    fn new() -> Self {
        MemStorage {
            sq_no: 0,
            mem_storage: HashMap::new(),
        }
    }

    fn read(&self, key: String) -> Option<KvOutput> {
        match self.mem_storage.get(&key) {
            Some(value) => {
                let output = KvOutput {
                    key: key,
                    value: value.clone(),
                };
                return Some(output);
            }
            None => return None,
        }
    }

    fn write(&mut self, key: String, value: String, seq_no: u64) -> KvOutput {
        self.mem_storage.insert(key.clone(), value.clone());
        self.sq_no = seq_no;

        KvOutput { key, value }
    }

    fn delete(&mut self, key: String, seq_no: u64) -> Option<KvOutput> {
        self.sq_no = seq_no;

        match self.mem_storage.remove(&key) {
            Some(removed_value) => {
                let output = KvOutput {
                    key: key,
                    value: removed_value,
                };

                Some(output)
            }
            None => None,
        }
    }

    fn fetch_all(&self) -> HashMap<String, String> {
        self.mem_storage.clone()
    }
}

/// ## KvOutput
/// KvOutput is the output containing the key-value pair after performing operation on KV Store via StoreEngine
pub struct KvOutput {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct WalRecord {
    length: u64,
    crc32: u32,
    payload: Vec<u8>,
}

impl WalRecord {
    fn new(wal: Wal) -> Self {
        let payload = wal.encode_to_vec();

        let length = payload.len() as u64;

        let mut hasher = Hasher::new();

        hasher.update(&payload);

        let crc32 = hasher.finalize();

        WalRecord {
            length,
            crc32,
            payload,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        // Real Wal Record
        let mut encoded_wal_record = Vec::with_capacity(8 + 4 + self.payload.len());

        let length_buf = self.length.to_le_bytes();

        // Append Bytes in encoded_wal_record Bytes Vector

        // First Length of Wal (Payload)
        encoded_wal_record.extend_from_slice(&length_buf);

        // Then CRC for integrity
        encoded_wal_record.extend_from_slice(&self.crc32.to_le_bytes());

        // At Last, Wal (Payload) itself
        encoded_wal_record.extend(self.payload);

        encoded_wal_record
    }

    /// reads the Wal Record from the File
    ///
    /// for the first from_file() call, the cursor is on the start of the file, and reads the first WalRecord, if it exists.
    /// then for later from_file() call, the cursor is just after the latest WalRecord readed by itself, this call will read next WalRecord, if it exists.
    ///
    ///
    /// ## Understanding how it works
    ///
    /// **First call**
    ///
    ///     +--------------------------------------------------+
    ///     
    ///     | R1 | R2 | R3 | R4 | R5 |
    ///     
    ///     ^
    ///     
    ///     Cursor
    ///----------------------------------------------------------
    /// **Second call**
    ///     
    ///     +--------------------------------------------------+
    ///     
    ///     | R1 | R2 | R3 | R4 | R5 |
    ///     
    ///           ^
    ///     
    ///           Cursor
    /// ----------------------------------------------------------
    /// **Thrid call**
    ///
    ///     +--------------------------------------------------+
    ///
    ///     | R1 | R2 | R3 | R4 | R5 |
    ///
    ///                ^
    ///
    ///                Cursor
    ///
    /// ----------------------------------------------------------
    /// The cursor automatically advances, so that next call reads the next WalRecord, if it exists
    ///
    /// ## Returns
    /// *Ok(Some(WalRecord))* -> when WalRecord is encountered in wal.log file
    ///
    ///         or
    ///  
    /// *Ok(None)* -> when EOF (End Of File) is encountered
    ///
    ///         or
    ///
    /// *Err(String)* -> when a Error is encountered
    fn from_file<R: Read>(reader: &mut R) -> Result<Option<Self>, String> {
        let mut length_buf = [0u8; 8];

        if let Err(e) = reader.read_exact(&mut length_buf) {
            if e.kind() == UnexpectedEof {
                return Ok(None);
            } else {
                return Err(get_err(format!(
                    "Failed to read length\nMost Probably the wal.log must be modified by external process\nOr the wal.log maybe corrupted {e}"
                )));
            }
        }

        let length = u64::from_le_bytes(length_buf);

        let mut crc32_buf = [0u8; 4];

        if let Err(e) = reader.read_exact(&mut crc32_buf) {
            return Err(get_err(format!(
                "Failed to read crc\nMost Probably the wal.log must be modified by external process\nOr the wal.log maybe corrupted {e}"
            )));
        }

        let stored_crc32 = u32::from_le_bytes(crc32_buf);

        let mut payload = vec![0u8; length as usize];

        if let Err(e) = reader.read_exact(&mut payload) {
            return Err(get_err(format!(
                "Failed to read WAL (payload)\nMost Probably the wal.log must be modified by external process\nOr the wal.log maybe corrupted {e}"
            )));
        }

        // Check for Integrity of payload

        let mut hasher = Hasher::new();

        hasher.update(&payload);

        let calculated_crc32 = hasher.finalize();

        if calculated_crc32 != stored_crc32 {
            return Err(get_err(format!(
                "CRC32 mismatch! Expected {}, got {}\nThe Intergrity of payload (operation) is compromised",
                stored_crc32, calculated_crc32
            )));
        }

        Ok(Some(WalRecord {
            length,
            crc32: stored_crc32,
            payload,
        }))
    }

    fn into_wal(self) -> Result<Wal, String> {
        let wal_buf = self.payload.as_slice();
        match Wal::decode(wal_buf) {
            Ok(wal) => Ok(wal),
            Err(e) => Err(get_err(format!(
                "Decoding the wal_buf from wal_record failed\n{:#?}\n\nError: {e}",
                self
            ))),
        }
    }
}

/// This keeps track of Access to KvStore \[StoreEngine\]
#[derive(Debug)]
struct AccessTracker {
    /// We will check ususally after 2 minutes if this accessed,
    ///
    /// **Not Accessed**: Flush from Memory
    ///
    /// **Accessed**: Calculate Avg and set ** back to 0
    current_access_count: u64,

    /// Average Number of time accessed per minute
    avg_access_per_minute: u64,

    /// Count of Intervals Passed
    intervals_count: u64,
}

impl AccessTracker {
    fn new_wrapped_in_mutex() -> Mutex<Self> {
        Mutex::new(AccessTracker {
            current_access_count: 0,
            avg_access_per_minute: 0,
            intervals_count: 0,
        })
    }
}

impl Snapshot {
    fn from_snapshot_file<R: Read>(mut reader: R) -> Result<Snapshot, String> {
        let mut buffer = Vec::new();

        reader
            .read_to_end(&mut buffer)
            .map_err(|e| get_err(format!("Failed to decode snapshot: {e}")))?;

        Snapshot::decode(buffer.as_slice())
            .map_err(|e| get_err(format!("Failed to decode snapshot: {e}")))
    }

    fn from_wal_file(mut wal_log_file: &File) -> Result<Snapshot, String> {
        let mut snapshot = Snapshot {
            last_sequence: 0,
            data: HashMap::new(),
        };

        loop {
            let Some(wal_record) = WalRecord::from_file(&mut wal_log_file)? else {
                break;
            };

            let wal = wal_record.into_wal()?;

            snapshot.execute_operation(wal)?;
        }

        Ok(snapshot)
    }

    fn perform_wal_log_operations(&mut self, mut wal_log_file: &File) -> Result<(), String> {
        let mut expected_sq_no = self.last_sequence + 1;
        loop {
            let Some(wal_record) = WalRecord::from_file(&mut wal_log_file)? else {
                break;
            };

            let wal = wal_record.into_wal()?;

            // If Operation in Wal is behind the snapshot, so there's no need to run the operation
            if self.last_sequence >= wal.sq_no {
                continue;
            }

            // Check for sequence number
            if wal.sq_no != expected_sq_no {
                return Err(get_err(format!(
                    "The wal.log is out of sync!, There are operations are missing.\nExpected seq_no: {expected_sq_no}\nCurrent Wal's sq_no: {}",
                    wal.sq_no
                )));
            }

            self.execute_operation(wal)?;

            // Increment the expected sequence number
            expected_sq_no += 1;
        }

        Ok(())
    }

    fn execute_operation(&mut self, wal: Wal) -> Result<(), String> {
        let av_op = match wal.operation {
            Some(op) => op,
            None => {
                return Err(get_err(format!(
                    "There was no available_operation found in Wal of sq_no: {}, it's your choice to ignore it or declare wal.log corrupted",
                    wal.sq_no
                )));
            }
        };

        match av_op.op {
            Some(Op::WriteOp(write_op)) => {
                self.data.insert(write_op.key, write_op.value);
            }
            Some(Op::DeleteOp(delete_op)) => {
                self.data.remove(&delete_op.key);
            }
            Some(Op::ReadOp(_)) => {
                return Err(get_err(format!(
                    "As only write and delete operations (which changes state of KV Store) are recorded on wal.log\nBut there was read operation found in Wal of sq_no: {}, it's your choice to ignore it or declare wal.log corrupted",
                    wal.sq_no
                )));
            }
            None => {
                return Err(get_err(format!(
                    "There was no op (operation) found in Wal of sq_no: {}, it's your choice to ignore it or declare wal.log corrupted",
                    wal.sq_no
                )));
            }
        }

        self.last_sequence = wal.sq_no;

        Ok(())
    }
}

#[derive(Debug)]
struct LogTracker {
    wal_file_path: String,
    /// ### next_seq_no
    ///
    /// when WAL instance (operation) is arrived, next_seq_no will be assigned to seq_no of WAL instance and then append it to wal.log, then next_seq_no will be assigned to seq_no of in_memory_storage and the operation will be performed on the hashmap in it.
    ///
    /// **And then don't forget to increment the next_seq_no**
    next_seq_no: u64,

    /// ### wal_file
    /// WAL_file is wal.log file which logs operations performed on KV Store in it.
    /// wal.log is stored in a wal folder
    wal_file: Option<File>,
}

impl LogTracker {
    fn new_wrapped_in_mutex(
        next_seq_no: u64,
        wal_file: File,
        wal_file_path: String,
    ) -> TokioMutex<Self> {
        TokioMutex::new(LogTracker {
            wal_file_path,
            next_seq_no, //This has to be a natural number i.e > 0, even 0 is not allowed
            wal_file: Some(wal_file),
        })
    }
}

#[derive(Debug)]
struct SnapshotTracker {
    gate: RwLock<()>,
    notification: Arc<Notify>,
    window_size: u64,
    next_seq: AtomicU64,
    due: AtomicBool,
}

impl SnapshotTracker {
    fn new(next_seq_no: u64, window_size: u64) -> Self {
        SnapshotTracker {
            gate: RwLock::new(()),
            notification: Arc::new(Notify::new()),
            window_size,
            next_seq: AtomicU64::new(next_seq_no + window_size),
            due: AtomicBool::new(false),
        }
    }

    fn spawn_snapshot_tracker(&self, weak_store_engine: Weak<StoreEngine>) {
        let notification_clone = Arc::clone(&self.notification);
        tokio::spawn(async move {
            loop {
                notification_clone.notified().await;

                let strong_store_engine = weak_store_engine.upgrade();

                if !strong_store_engine.is_none() {
                    let strong_store_engine = strong_store_engine.unwrap();

                    let snapshot_tracker = &strong_store_engine.snapshot_tracker;

                    let _gate = snapshot_tracker.gate.write().await;

                    if let Err(e) = strong_store_engine.write_and_save_snapshot().await {
                        eprintln!("{e}");
                        continue;
                    }

                    // Update the next_seq in SnapshotTracker
                    let log_tracker = strong_store_engine.log_tracker.lock().await;

                    snapshot_tracker.next_seq.store(
                        log_tracker.next_seq_no + snapshot_tracker.window_size,
                        Ordering::Relaxed,
                    );

                    snapshot_tracker.due.store(false, Ordering::Release);
                } else {
                    break;
                }
            }
        });
    }

    fn check_threshold(&self, next_seq_no: u64) {
        if next_seq_no > self.next_seq.load(Ordering::Relaxed) {
            if !self.due.swap(true, Ordering::AcqRel) {
                self.notification.notify_one();
            }
        }
    }
}

/// ## StoreEngine
/// StoreEngine manages respective KV Store.
///
/// StoreEngine creates a 'projects/kv_store_name' folder consisting of it's WAL log and latest snapshot.
///
/// Maintains WAL.log, snapshot.bin, recovery, etc.
///
/// # Example
/// ```
/// use multithreaded_kv_store::StoreEngine;
///
/// let my_kv_store_engine = StoreEngine::connect("my_kv");
///
/// ```
#[derive(Debug)]
pub struct StoreEngine {
    kv_store_name: String,

    in_memory_storage: RwLock<MemStorage>,

    log_tracker: TokioMutex<LogTracker>,

    access_tracker: Mutex<AccessTracker>,

    snapshot_tracker: SnapshotTracker,
}

impl StoreEngine {
    /// connect() associative function connects to instance of StoreEngine for the given 'kv_store_name'
    /// project is part of Store
    ///
    /// **snapshot_window_size**: After how many operations should snapshot trigger, it doesn't mean that exactly only that amount of operations will be saved in the snapshot, as it will only trigger the snapshot but still wait for ongoing operations to complete and then create new snapshot
    ///
    /// # Example
    /// ```
    /// let my_kv_store_engine = StoreEngine::connect("my_kv", 65000);
    /// ```
    pub fn connect(kv_store_name: &str, snapshot_window_size: u64) -> Result<Arc<Self>, String> {
        let wal_folder_path = format!("projects/{kv_store_name}/wal");

        if let Err(e) = create_dir_all(&wal_folder_path) {
            return Err(get_err(format!(
                "Failed to ensure directory exists\nDirectory Path: {}\n{e}",
                &wal_folder_path
            )));
        }

        let wal_file_path = wal_folder_path + "/wal.log";

        match File::options()
            .read(true)
            .write(true)
            .append(true)
            .open(&wal_file_path)
        {
            Ok(file) => {
                println!("Starting {kv_store_name} KV Store's Recovery!");

                return StoreEngine::recovery_restart(
                    kv_store_name,
                    snapshot_window_size,
                    file,
                    wal_file_path,
                );
            }

            Err(e) if e.kind() == ErrorKind::NotFound => {
                println!("Creating {kv_store_name} KV Store!!");

                match OpenOptions::new()
                    .create_new(true) // Fails if file exists
                    .read(true)
                    .write(true)
                    .append(true) // Open in append mode
                    .open(&wal_file_path)
                {
                    Ok(wal_file) => {
                        let next_seq_no = 1;

                        let store_engine = Arc::new(StoreEngine {
                            kv_store_name: kv_store_name.to_string(),

                            in_memory_storage: RwLock::new(MemStorage::new()),

                            log_tracker: LogTracker::new_wrapped_in_mutex(
                                next_seq_no,
                                wal_file,
                                wal_file_path,
                            ),

                            access_tracker: AccessTracker::new_wrapped_in_mutex(),

                            snapshot_tracker: SnapshotTracker::new(
                                next_seq_no,
                                snapshot_window_size,
                            ),
                        });

                        store_engine
                            .snapshot_tracker
                            .spawn_snapshot_tracker(Arc::downgrade(&store_engine));

                        Ok(store_engine)
                    }
                    Err(e) => Err(get_err(format!(
                        "Error Occured while creating new wal.log for {kv_store_name} project\n{e}"
                    ))),
                }
            }

            Err(e) => {
                return Err(get_err(format!(
                    "Error Occured while creating wal.log file.\n{e}"
                )));
            }
        }
    }

    fn recovery_restart(
        kv_store_name: &str,
        snapshot_window_size: u64,
        wal_log_file: File,
        wal_file_path: String,
    ) -> Result<Arc<Self>, String> {
        //Recovery means getting the in_memory_storage in the correct latest state using log(s) and snapshot(s)
        let mut snapshot: Snapshot;
        let snapshot_path = format!("projects/{}/snapshot/snapshot.bin", kv_store_name);

        match File::open(&snapshot_path) {
            Ok(snapshot_file) => {
                snapshot = Snapshot::from_snapshot_file(snapshot_file)?;

                snapshot.perform_wal_log_operations(&wal_log_file)?;
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                snapshot = Snapshot::from_wal_file(&wal_log_file)?;
            }
            Err(e) => {
                return Err(get_err(format!(
                    "Failed to open snapshot.bin\nPath: {snapshot_path}\n\nError: {e}"
                )));
            }
        }

        let next_seq_no = snapshot.last_sequence + 1;

        let store_engine = Arc::new(StoreEngine {
            kv_store_name: kv_store_name.to_string(),
            in_memory_storage: RwLock::new(MemStorage {
                sq_no: snapshot.last_sequence,
                mem_storage: snapshot.data,
            }),
            log_tracker: LogTracker::new_wrapped_in_mutex(next_seq_no, wal_log_file, wal_file_path),
            access_tracker: AccessTracker::new_wrapped_in_mutex(),
            snapshot_tracker: SnapshotTracker::new(next_seq_no, snapshot_window_size),
        });

        store_engine
            .snapshot_tracker
            .spawn_snapshot_tracker(Arc::downgrade(&store_engine));

        return Ok(store_engine);
    }

    /// *prepare_operation* returns an *AvailableOperationsOnKv* instance for the given *operation_query*.
    /// Returns Syntax Error if query is incorrect
    /// ## Example
    /// ```
    /// use multithreaded_kv_store::StoreEngine;
    /// let prepared_operation = StoreEngine::prepare_operation("Read Name").unwrap();
    /// ```
    ///
    /// # RULES of operation_query
    ///
    /// ## Write Operation
    ///
    /// "Write \<key\> = \<value\>"
    ///
    /// Example:
    ///
    /// "Write name = Amey"
    ///
    /// ## Read Operation
    ///
    /// "Read \<key\>"
    ///
    /// Example:
    ///
    /// "Read name"
    ///
    /// ## Delete Operation
    ///
    /// "Delete \<key\>"
    ///
    /// Example:
    ///
    /// "Delete name"
    ///
    /// ## Fetch All Operation
    ///
    /// "Fetch All"
    ///
    ///
    /// ## Keywords
    /// Write, Read, Delete, etc,. keywords are case-insensitive
    ///
    /// ## Keys and Values
    /// Keys i.e \<key\> and Values i.e \<value\> are case-sensitive
    ///
    /// i.e name, Name, NAme are not equal, therefore, 3 distinct keys
    ///
    pub fn prepare_operation(
        &self,
        operation_query: &str,
    ) -> Result<AvailableOperationsOnKv, String> {
        // let operation_query_lowered = operation_query.to_lowercase();
        let query_list: Vec<&str> = operation_query.split(' ').collect();

        if let Some(operation_from_query) = query_list.get(0) {
            let op: Op;
            let operation_from_query_lowered = &operation_from_query.to_lowercase()[..];

            match operation_from_query_lowered {
                "write" => {
                    if query_list.len() != 4 || query_list.get(2) != Some(&"=") {
                        return Err("Write Operation Syntax Error \n".to_string());
                    }
                    op = Op::WriteOp(WriteKv {
                        key: query_list.get(1).expect("Write Syntax Error").to_string(),
                        value: query_list.get(3).expect("Write Syntax Error").to_string(),
                    });
                }
                "read" => {
                    if query_list.len() != 2 {
                        return Err("Read Operation Syntax Error \n".to_string());
                    }
                    op = Op::ReadOp(ReadKv {
                        key: query_list.get(1).expect("Read Syntax Error").to_string(),
                    });
                }
                "delete" => {
                    if query_list.len() != 2 {
                        return Err("Delete Operation Syntax Error \n".to_string());
                    }
                    op = Op::DeleteOp(DeleteKv {
                        key: query_list.get(1).expect("Delete Syntax Error").to_string(),
                    });
                }
                _ => {
                    return Err("Syntax Error:\nIllegal Query, Coundn't find any operation in the provided query, please write query using correct syntax.".to_string());
                }
            }

            Ok(AvailableOperationsOnKv { op: Some(op) })
        } else {
            return Err("Syntax Error:\nIllegal Query, Coundn't find any operation in query, please write query using correct syntax.".to_string());
        }
    }

    /// ## execute_operation()
    /// Use execute_operation() to execute operation on a KvStore via StoreEngine
    ///
    /// ## Example
    /// ```
    /// use multithreaded_kv_store::StoreEngine;
    ///
    /// let my_kv_store_engine = StoreEngine::connect("my_kv", 65000);
    ///
    /// let operation = StoreEngine::prepare_operation("Read name").unwrap();
    ///
    /// let output = my_kv_store_engine.execute_operation(operation).unwrap();
    ///
    /// ```
    ///
    pub async fn execute_operation(
        &self,
        mut operation: AvailableOperationsOnKv,
    ) -> Result<Option<KvOutput>, String> {
        self.access_tracker.lock().unwrap().current_access_count += 1;

        match operation.op.take() {
            Some(Op::ReadOp(read_op)) => {
                let _gate = self.snapshot_tracker.gate.read().await;

                Ok(self.in_memory_storage.read().await.read(read_op.key))
            }

            // For WriteOp and DeleteOp we must write/append these operations on log (file), making sure that the state of KvStore is recoverable
            // We take snpashot_gate's read_lock so that,
            // when read + write/delete operations are there read can run freely and write/delete can concurrently append on wal.log then wait for read to finish and write/delete on in_memory_storage through write_lock
            // when write + delete operations are occured, the order of request will determine which gets first mutex_lock on log_tracker and then to write_lock of in_memory_storage making it sequential
            // Therefore WriteOp / DeleteOp consume 1 sequence number even if WriteOp is with the existing Key and value or DeleteOp is with nonexisting key in the in_memory_storage
            Some(Op::WriteOp(write_kv)) => {
                let _gate = self.snapshot_tracker.gate.read().await;

                let mut log_tracker_guard = self.log_tracker.lock().await;

                self.record_on_wal_log(&operation, &mut log_tracker_guard)
                    .await?;

                let output: KvOutput = self.in_memory_storage.write().await.write(
                    write_kv.key,
                    write_kv.value,
                    log_tracker_guard.next_seq_no,
                );

                log_tracker_guard.next_seq_no += 1;

                self.snapshot_tracker
                    .check_threshold(log_tracker_guard.next_seq_no);

                return Ok(Some(output));
            }
            Some(Op::DeleteOp(delete_kv)) => {
                let _gate = self.snapshot_tracker.gate.read().await;

                let mut log_tracker_guard = self.log_tracker.lock().await;

                self.record_on_wal_log(&operation, &mut log_tracker_guard)
                    .await?;

                match self
                    .in_memory_storage
                    .write()
                    .await
                    .delete(delete_kv.key, log_tracker_guard.next_seq_no)
                {
                    Some(output) => {
                        log_tracker_guard.next_seq_no += 1;

                        self.snapshot_tracker
                            .check_threshold(log_tracker_guard.next_seq_no);

                        return Ok(Some(output));
                    }
                    None => {
                        log_tracker_guard.next_seq_no += 1;

                        self.snapshot_tracker
                            .check_threshold(log_tracker_guard.next_seq_no);

                        return Ok(None);
                    }
                }
            }

            None => return Err(get_err("Operation is None!".to_string())),
        }

        // self.in_memory_storage.mem_storage.insert(, v);
        // Ok(())
    }

    /// Atomically replace `dst` with `src` and make the rename durable.
    ///
    /// On Windows:
    ///     MoveFileExW + MOVEFILE_WRITE_THROUGH
    ///
    /// On Unix:
    ///     rename + fsync(directory)
    fn durable_snapshot_rename(&self, src: &Path, dst: &Path) -> Result<(), String> {
        #[cfg(windows)]
        {
            let src_wide: Vec<u16> = src
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();

            let dst_wide: Vec<u16> = dst
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();

            let result = unsafe {
                MoveFileExW(
                    src_wide.as_ptr(),
                    dst_wide.as_ptr(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            };

            if result == 0 {
                return Err(get_err(format!(
                    "\nError occured while renaming file\n\nError: {}",
                    std::io::Error::last_os_error()
                )));
            }

            Ok(())
        }

        #[cfg(unix)]
        {
            fs::rename(src, dst)?;

            // Make the directory entry update durable.
            let dir = fs::File::open(src.parent().ok_or_else(|| {
                let io_error = io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "source path has no parent directory",
                );

                return Err(get_err(format!(
                    "Error occured while fetching the parent directory\n\nError: {io_error}"
                )));
            })?)?;

            dir.sync_all()?;

            Ok(())
        }
    }

    async fn create_and_write_snapshot_temp(&self) -> Result<(), String> {
        let snapshot_folder_path = format!("projects/{}/snapshot", self.kv_store_name);

        let in_memory_storage_read_guard = self.in_memory_storage.read().await;

        let snapshot = Snapshot {
            last_sequence: in_memory_storage_read_guard.sq_no,
            data: in_memory_storage_read_guard.mem_storage.clone(),
        };

        let mut snapshot_buf = Vec::new();

        // Encoding snapshot into snapshot buffer
        if let Err(e) = snapshot.encode(&mut snapshot_buf) {
            return Err(get_err(format!(
                "Snapshot Encoding Failed\nSnapshot: {:#?}\n\nError: {e}",
                snapshot
            )));
        }

        // create snapshot directory if doesn't exists
        if let Err(e) = create_dir_all(&snapshot_folder_path) {
            return Err(get_err(format!(
                "Failed to ensure directory exists\nDirectory Path: {}\n{e}",
                &snapshot_folder_path
            )));
        }

        let temp_snapshot_file_path = snapshot_folder_path.clone() + "/snapshot.tmp";
        // Create new temp_snapshot_file in snapshot directory
        match File::create(&temp_snapshot_file_path) {
            Ok(mut temp_snapshot_file) => {
                // Write on
                if let Err(e) = temp_snapshot_file.write_all(&snapshot_buf) {
                    return Err(get_err(format!(
                        "Failed to write on temp_snapshot_file\n Error: {e}"
                    )));
                }

                // sync data as well as metadata of snapshot.bin file on the disk
                if let Err(e) = temp_snapshot_file.sync_all() {
                    return Err(get_err(format!(
                        "Error Occured while syncing the temp_snapshot_file of {} project\n{e}",
                        self.kv_store_name
                    )));
                }
            }
            Err(e) => {
                return Err(get_err(format!(
                    "Failed to create snapshot.tmp\n{temp_snapshot_file_path}\n\nError: {e}"
                )));
            }
        }
        Ok(())
    }

    async fn truncate_wal_log(&self) -> Result<(), String> {
        let mut log_tracker_guard = self.log_tracker.lock().await;

        // remove and drop wal_file's instance from log_tracker, then open it by truncating
        drop(log_tracker_guard.wal_file.take());

        let truncated_wal_log_file = match File::options()
            .read(true)
            .write(true)
            .truncate(true)
            .open(&log_tracker_guard.wal_file_path)
        {
            Ok(truncated_wal_log_file) => truncated_wal_log_file,
            Err(e) => {
                return Err(get_err(format!(
                    "Failed to truncate wal.log file\nwal_file_path: {}\n\nError: {e}",
                    log_tracker_guard.wal_file_path
                )));
            }
        };

        // sync_all make sures that file is actually updated on the disk
        if let Err(e) = truncated_wal_log_file.sync_all() {
            return Err(get_err(format!(
                "Error Occured while syncing the truncated wal.log file of {} project\n\nError: {e}",
                &self.kv_store_name
            )));
        }

        drop(truncated_wal_log_file);

        match File::options()
            .read(true)
            .write(true)
            .append(true)
            .open(&log_tracker_guard.wal_file_path)
        {
            Ok(truncated_wal_log_file) => {
                // then palce the truncated wal_log_file back into log_tracker
                log_tracker_guard.wal_file = Some(truncated_wal_log_file);
            }
            Err(e) => {
                return Err(get_err(format!("Failed to open wal.log in append-only mode\nwal_file_path: {}\n\nError: {e}", log_tracker_guard.wal_file_path)));
            }
        }

        // if let Err(e) = log_tracker_guard.wal_file.set_len(0) {
        //     return Err(get_err(format!(
        //         "Failed to truncate wal.log file\n\nFile: {:#?}\n\nError: {e}",
        //         log_tracker_guard.wal_file
        //     )));
        // }

        // // sync data as well as metadata of wal.log file on the disk
        // if let Err(e) = log_tracker_guard.wal_file.sync_all() {
        //     return Err(get_err(format!(
        //         "Error Occured while syncing the wal.log of {} project\n{e}",
        //         self.kv_store_name
        //     )));
        // }

        Ok(())
    }

    fn rename_snapshot_to_save_it(&self) -> Result<(), String> {
        let snapshot_folder_path = format!("projects/{}/snapshot", self.kv_store_name);

        let temp_snapshot_file_path = snapshot_folder_path.clone() + "/snapshot.tmp";

        let real_snapshot_file_path = snapshot_folder_path.clone() + "/snapshot.bin";

        if let Err(e) = self.durable_snapshot_rename(
            Path::new(&temp_snapshot_file_path),
            Path::new(&real_snapshot_file_path),
        ) {
            return Err(get_err(format!(
                "Failed to rename snapshot.tmp to snapshot.bin\n\ntemp_snapshot_file_path: {temp_snapshot_file_path}\nreal_snapshot_file_path: {real_snapshot_file_path}\n\nError: {e}"
            )));
        }

        Ok(())
    }

    /// writes the in_memory_storage into the snapshot.bin atomically
    pub async fn write_and_save_snapshot(&self) -> Result<(), String> {
        self.create_and_write_snapshot_temp().await?;
        self.rename_snapshot_to_save_it()?;
        self.truncate_wal_log().await?;

        Ok(())
    }

    /// Creates updated snapshot, then truncates wal.log and releases the memory allocated to this StoreEngine
    pub async fn graceful_shutdown(&self) -> Result<(), String> {
        let _gate = self.snapshot_tracker.gate.write().await;
        self.write_and_save_snapshot().await
    }

    /// ## Returns
    ///
    /// *KvOutput* if key **exists**,
    /// otherwise *None*
    pub async fn read(&self, key: String) -> Option<KvOutput> {
        let _gate = self.snapshot_tracker.gate.read().await;
        let in_memory_storage_read_guard = self.in_memory_storage.read().await;
        in_memory_storage_read_guard.read(key)
    }

    /// ## Returns
    ///
    /// Ok -> Some -> *KvOutput* when write is successful,
    ///
    /// *Err(e)* when Error occurs, failing the write operation
    pub async fn write(&self, key: String, value: String) -> Result<Option<KvOutput>, String> {
        let _gate = self.snapshot_tracker.gate.read().await;

        let op = Op::WriteOp(WriteKv {
            key: key.clone(),
            value: value.clone(),
        });

        let operation = AvailableOperationsOnKv { op: Some(op) };

        let mut log_tracker_guard = self.log_tracker.lock().await;

        self.record_on_wal_log(&operation, &mut log_tracker_guard)
            .await?;

        let mut in_memory_storage_write_guard = self.in_memory_storage.write().await;
        let output: KvOutput =
            in_memory_storage_write_guard.write(key, value, log_tracker_guard.next_seq_no);

        log_tracker_guard.next_seq_no += 1;

        self.snapshot_tracker
            .check_threshold(log_tracker_guard.next_seq_no);

        return Ok(Some(output));
    }

    /// ## Returns
    ///
    /// Ok -> Some -> *KvOutput* if key **exists** and delete operation is **successful**,
    ///
    /// Ok -> None if key doesn't **exists** in the KvStore
    ///
    /// *Err(e)* when Error occurs, failing the delete operation
    pub async fn delete(&self, key: String) -> Result<Option<KvOutput>, String> {
        let _gate = self.snapshot_tracker.gate.read().await;

        let op = Op::DeleteOp(DeleteKv { key: key.clone() });

        let operation = AvailableOperationsOnKv { op: Some(op) };

        let mut log_tracker_guard = self.log_tracker.lock().await;

        self.record_on_wal_log(&operation, &mut log_tracker_guard)
            .await?;

        let mut in_memory_storage_write_guard = self.in_memory_storage.write().await;

        match in_memory_storage_write_guard.delete(key, log_tracker_guard.next_seq_no) {
            Some(output) => {
                log_tracker_guard.next_seq_no += 1;

                self.snapshot_tracker
                    .check_threshold(log_tracker_guard.next_seq_no);
                return Ok(Some(output));
            }
            None => {
                log_tracker_guard.next_seq_no += 1;

                self.snapshot_tracker
                    .check_threshold(log_tracker_guard.next_seq_no);
                return Ok(None);
            }
        }
    }

    pub async fn fetch_all(&self) -> HashMap<String, String> {
        let in_memory_storage_read_guard = self.in_memory_storage.read().await;
        in_memory_storage_read_guard.fetch_all()
    }

    async fn record_on_wal_log(
        &self,
        operation: &AvailableOperationsOnKv,
        log_tracker_guard: &mut LogTracker,
    ) -> Result<(), String> {
        // Creating & Coverting Wal, len, crc32 into Bytes Slice or Vector
        let wal = Wal {
            sq_no: log_tracker_guard.next_seq_no,
            operation: Some(operation.clone()),
        };
        let wal_record = WalRecord::new(wal);

        let encoded_wal_record = wal_record.into_bytes();

        match log_tracker_guard.wal_file.as_mut() {
            Some(wal_file) => {
                //appending encoded encoded_wal_record to wal.log file
                if let Err(e) = wal_file.write_all(&encoded_wal_record) {
                    return Err(get_err(format!(
                        "Error Occured while writing wal buffer on wal.log file of {} project\n{e}",
                        &self.kv_store_name
                    )));
                }

                // sync only the data (content) of wal.log file on the disk
                // use sync_all instead of sync_data if you want to sync data as well as metadata fo wal.log file on the disk
                if let Err(e) = wal_file.sync_data() {
                    return Err(get_err(format!(
                        "Error Occured while syncing the wal.log file of {} project\n{e}",
                        &self.kv_store_name
                    )));
                }
            }
            None => {
                return Err(get_err(format!(
                    "log_tracker doesn't have wal.log File's instance"
                )));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Stores {
    inner: HashMap<String, Arc<StoreEngine>>,
}

/// A HashMap of currently active StoreEngines
pub type StoresMap = Arc<RwLock<Stores>>;

impl Stores {
    /// Creates new instance of Stores
    pub fn new() -> Stores {
        Stores {
            inner: HashMap::new(),
        }
    }

    /// Inserts KvStore of *kv_store_name* in the StoresMap
    /// ## Example
    /// ```
    /// // Define StoresMap
    /// let mut stores_map:StoresMap;
    ///
    /// stores_map.insert("my_kv").await.unwrap();
    /// ```
    pub fn insert(&mut self, kv_store_name: &str, store_engine: Arc<StoreEngine>) {
        // By default the snapshot_window_size is 65000 operations
        self.inner.insert(kv_store_name.to_string(), store_engine);
    }

    /// fetches the StoreEnigne of the given kv_store_name from the StoresMap
    /// ## Example
    /// ```
    /// // Fetch & use from Stores
    /// let stores = stores_map.clone();
    ///
    /// let fetch = move || {
    ///     let stores = stores.lock().unwrap(); // <-- HashMap lock acquired here
    ///
    ///     stores.get("store1").clone() // returns the StoreEnigne's Arc pointer
    /// }; // <-- HashMap lock released here
    ///
    /// // Get lock of the StoreEnigne
    /// let mut engine = fetch().lock().unwrap();
    ///
    /// engine.execute_operation(...);
    ///  
    /// ```
    pub fn get(&self, kv_store_name: &str) -> Option<Arc<StoreEngine>> {
        println!("{:#?}", self.inner);
        if let Some(locked_store_engine) = self.inner.get(kv_store_name) {
            Some(Arc::clone(locked_store_engine))
        } else {
            None
        }
    }

    fn update_access_tracker(&mut self) {
        self.inner.retain(|_, store_engine| {
            let mut access_tracker = store_engine.access_tracker.lock().unwrap();

            if access_tracker.current_access_count != 0 {
                access_tracker.avg_access_per_minute += (access_tracker.current_access_count
                    - access_tracker.avg_access_per_minute)
                    / access_tracker.intervals_count;
                access_tracker.current_access_count = 0;
                access_tracker.intervals_count += 1;
                true
            } else {
                false
            }
        });
    }

    pub async fn graceful_shutdown(&self) -> Result<(), String> {
        let store_engines: Vec<Arc<StoreEngine>> = self.inner.values().cloned().collect();

        for store_engine in store_engines {
            store_engine.graceful_shutdown().await?;
        }

        Ok(())
    }
}

/// ## get_err()
/// allows you to print your custom message with the actual error given by the rust.
///
/// ### Example
/// ```
/// fn foo(wal: Wal) -> Result<Vec<u8>, String> {
///
///     let mut buf = Vec::new();
///     if let Err(e) = wal.encode(&mut buf) {
///         return Err(get_err(format!(
///             "Error Occured while encoding operation into 8bit buffer (vec<u8>)\n{e}"
///         )));
///     }
/// }
/// ```
pub fn get_err(err: String) -> String {
    println!("{}", &err);
    return err;
}
