// Inbuild libs
use prost::Message;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, create_dir_all},
    io::{ErrorKind, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::interval;

//Protocol Buffer Data Structures
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/kvstore.rs"));
}
pub use proto::{DeleteKv, ReadKv, Wal, WriteKv, available_operations_on_kv::Op};

use proto::AvailableOperationsOnKv;

#[derive(Clone)]
pub struct AppState {
    stores_map: StoresMap,
    /// true -> Working/On , false -> Stopped/Off
    access_tracker_flag: Arc<AtomicBool>,
}

impl AppState {
    pub fn new() -> AppState {
        AppState {
            stores_map: StoresMap::new(Mutex::new(Stores::new())),
            access_tracker_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn get_stores_map_clone(&self) -> StoresMap {
        Arc::clone(&self.stores_map)
    }

    /// access_tracker stops
    pub fn stop_access_tracker(&mut self) {
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

                let mut stores_map_guard = stores_map.lock().unwrap();

                stores_map_guard.update_access_tracker();
            }
        });
    }
}

// /// ### Operation
// /// Available Operations that can be performed on the KV Store
// enum Operation {
//     Write { key: String, value: String },
//     Read { key: String },
//     Delete { key: String },
// }

// /// ### WAL (Write Ahead Log)
// /// WAL is the format of operation stored in wal.log
// struct WAL {
//     sq_no: u64, //Sequence Number
//     operation: Option<Operation>,
// }

/// ### MemStorage
/// In-Memory Storage of KV
#[derive(Debug)]
struct MemStorage {
    sq_no: u64, //Sequence Number
    mem_storage: HashMap<String, String>,
}

impl MemStorage {
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
    fn new() -> Self {
        AccessTracker {
            current_access_count: 0,
            avg_access_per_minute: 0,
            intervals_count: 0,
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

    /// ### next_seq_no
    ///
    /// when WAL instance (operation) is arrived, next_seq_no will be assigned to seq_no of WAL instance and then append it to wal.log, then next_seq_no will be assigned to seq_no of in_memory_storage and the operation will be performed on the hashmap in it.
    ///
    /// **And then don't forget to increment the next_seq_no**
    next_seq_no: u64,

    in_memory_storage: MemStorage,

    /// ### wal_file
    /// WAL_file is wal.log file which logs operations performed on KV Store in it.
    /// wal.log is stored in a wal folder
    wal_file: File,
    /// ### snapshot
    /// snapshot is the HashMap stored in a file
    /// snapshot.bin is stored in a snapshots folder
    snapshot_file: Option<File>,

    access_tracker: AccessTracker,
}

impl StoreEngine {
    /// connect() associative function connects to instance of StoreEngine for the given 'kv_store_name'
    /// project is part of Store
    ///
    /// # Example
    /// ```
    /// let my_kv_store_engine = StoreEngine::connect("my_kv");
    /// ```
    pub fn connect(kv_store_name: &str) -> Result<Self, String> {
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
                return Ok(StoreEngine::recovery_restart(kv_store_name, file));
            }

            Err(e) if e.kind() == ErrorKind::NotFound => {
                println!("Creating {kv_store_name} KV Store!!");

                match OpenOptions::new()
                    .create_new(true) // Fail if file exists
                    .append(true) // Open in append mode
                    .open("output.txt")
                {
                    Ok(wal_file) => {
                        let store_engine = StoreEngine {
                            kv_store_name: kv_store_name.to_string(),
                            next_seq_no: 1, //This has to be a natural number i.e > 0, even 0 is not allowed
                            in_memory_storage: MemStorage {
                                sq_no: 0, //for now this value doesn't really matter
                                mem_storage: HashMap::new(),
                            },
                            wal_file,
                            snapshot_file: None,
                            access_tracker: AccessTracker::new(),
                        };
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

    fn recovery_restart(kv_store_name: &str, file: File) -> Self {
        //Recovery code goes here
        //Recovery means getting the in_memory_storage in the correct latest state using logs and snapshots

        return StoreEngine {
            kv_store_name: kv_store_name.to_string(),
            next_seq_no: 1, //This has to be a natural number i.e > 0, even 0 is not allowed
            in_memory_storage: MemStorage {
                sq_no: 0, //for now this value doesn't really matter
                mem_storage: HashMap::new(),
            },
            wal_file: file,
            snapshot_file: None,
            access_tracker: AccessTracker::new(),
        };
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
    /// let my_kv_store_engine = StoreEngine::connect("my_kv");
    ///
    /// let operation = StoreEngine::prepare_operation("Read name").unwrap();
    ///
    /// let output = my_kv_store_engine.execute_operation(operation).unwrap();
    ///
    /// ```
    ///
    pub fn execute_operation(
        &mut self,
        mut operation: AvailableOperationsOnKv,
    ) -> Result<Option<KvOutput>, String> {
        self.access_tracker.current_access_count += 1;

        match operation.op.take() {
            Some(op) => {
                match op {
                    //No need to 'write/append on log', as reading doesn't make any changes in the state of KvStore.
                    Op::ReadOp(read_op) => Ok(self.in_memory_storage.read(read_op.key)),

                    //For WriteOp and DeleteOp we must write/append these operations on log (file), making sure that the state of KvStore is recoverable
                    Op::WriteOp(write_kv) => match self.record_on_wal_log(&operation) {
                        Ok(()) => {
                            let output: KvOutput = self.in_memory_storage.write(
                                write_kv.key,
                                write_kv.value,
                                self.next_seq_no,
                            );

                            self.next_seq_no += 1;

                            return Ok(Some(output));
                        }
                        Err(e) => return Err(e),
                    },
                    Op::DeleteOp(delete_kv) => match self.record_on_wal_log(&operation) {
                        Ok(()) => match self
                            .in_memory_storage
                            .delete(delete_kv.key, self.next_seq_no)
                        {
                            Some(output) => {
                                self.next_seq_no += 1;
                                return Ok(Some(output));
                            }
                            None => {
                                self.next_seq_no += 1;
                                return Ok(None);
                            }
                        },
                        Err(e) => return Err(e),
                    },
                }
            }
            None => return Err(get_err("Operation is None!".to_string())),
        }

        // self.in_memory_storage.mem_storage.insert(, v);
        // Ok(())
    }

    /// ## Returns
    ///
    /// *KvOutput* if key **exists**,
    /// otherwise *None*
    pub fn read(&self, key: String) -> Option<KvOutput> {
        self.in_memory_storage.read(key)
    }

    /// ## Returns
    ///
    /// Ok -> Some -> *KvOutput* when write is successful,
    ///
    /// *Err(e)* when Error occurs, failing the write operation
    pub fn write(&mut self, key: String, value: String) -> Result<Option<KvOutput>, String> {
        let op = Op::WriteOp(WriteKv {
            key: key.clone(),
            value: value.clone(),
        });

        let operation = AvailableOperationsOnKv { op: Some(op) };

        // also records the sequence number in the log
        match self.record_on_wal_log(&operation) {
            Ok(()) => {
                let output: KvOutput = self.in_memory_storage.write(key, value, self.next_seq_no);

                self.next_seq_no += 1;

                return Ok(Some(output));
            }
            Err(e) => return Err(e),
        }
    }

    /// ## Returns
    ///
    /// Ok -> Some -> *KvOutput* if key **exists** and delete operation is **successful**,
    ///
    /// Ok -> None if key doesn't **exists** in the KvStore
    ///
    /// *Err(e)* when Error occurs, failing the delete operation
    pub fn delete(&mut self, key: String) -> Result<Option<KvOutput>, String> {
        let op = Op::DeleteOp(DeleteKv { key: key.clone() });

        let operation = AvailableOperationsOnKv { op: Some(op) };

        match self.record_on_wal_log(&operation) {
            Ok(()) => match self.in_memory_storage.delete(key, self.next_seq_no) {
                Some(output) => {
                    self.next_seq_no += 1;
                    return Ok(Some(output));
                }
                None => {
                    self.next_seq_no += 1;
                    return Ok(None);
                }
            },
            Err(e) => return Err(e),
        }
    }

    pub fn fetch_all(&self) -> HashMap<String, String> {
        self.in_memory_storage.fetch_all()
    }
    fn record_on_wal_log(&mut self, operation: &AvailableOperationsOnKv) -> Result<(), String> {
        let wal = Wal {
            sq_no: self.next_seq_no,
            operation: Some(operation.clone()),
        };

        let mut wal_buf = Vec::new();

        // filling buf with encoded operation
        if let Err(e) = wal.encode(&mut wal_buf) {
            return Err(get_err(format!(
                "Error Occured while encoding wal into 8bit wal buffer (vec<u8>)\n{e}"
            )));
        }

        //appending encoded operation to wal.log file
        if let Err(e) = self.wal_file.write_all(&wal_buf) {
            return Err(get_err(format!(
                "Error Occured while writing wal buffer on wal.log file of {} project\n{e}",
                &self.kv_store_name
            )));
        }

        // sync only the data (content) of wal.log file on the disk
        // use sync_all instead of sync_data if you want to sync data as well as metadata fo wal.log file on the disk
        if let Err(e) = self.wal_file.sync_data() {
            return Err(get_err(format!(
                "Error Occured while syncing the wal.log file of {} project\n{e}",
                &self.kv_store_name
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Stores {
    inner: HashMap<String, Arc<Mutex<StoreEngine>>>,
}

/// A HashMap of currently active StoreEngines
pub type StoresMap = Arc<Mutex<Stores>>;

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
    pub fn insert(&mut self, kv_store_name: &str) -> Result<(), String> {
        let store_engine: StoreEngine = StoreEngine::connect(kv_store_name)?;

        self.inner.insert(
            kv_store_name.to_string(),
            Arc::new(Mutex::new(store_engine)),
        );

        Ok(())
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
    pub fn get(&self, kv_store_name: &str) -> Option<Arc<Mutex<StoreEngine>>> {
        println!("{:#?}", self.inner);
        if let Some(locked_store_engine) = self.inner.get(kv_store_name) {
            Some(Arc::clone(locked_store_engine))
        } else {
            None
        }
    }

    pub fn update_access_tracker(&mut self) {
        self.inner.retain(|_, store_engine| {
            let mut store_engine = store_engine.lock().unwrap();

            if store_engine.access_tracker.current_access_count != 0 {
                store_engine.access_tracker.avg_access_per_minute +=
                    (store_engine.access_tracker.current_access_count
                        - store_engine.access_tracker.avg_access_per_minute)
                        / store_engine.access_tracker.intervals_count;
                store_engine.access_tracker.current_access_count = 0;
                store_engine.access_tracker.intervals_count += 1;
                true
            } else {
                false
            }
        });
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
