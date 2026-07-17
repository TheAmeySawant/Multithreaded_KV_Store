// Inbuild libs
use core::panic;
use std::{
    collections::HashMap,
    fs::{File, create_dir_all},
    io::ErrorKind,
};
use tokio;

//Protocol Buffer Data Structures
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/kvstore.rs"));
}
use proto::{
    AvailableOperationsOnKv, DeleteKv, ReadKv, Wal, WriteKv, available_operations_on_kv::Op,
};

impl AvailableOperationsOnKv {
    ///
    /// # PrepareOperation
    ///
    /// PrepareOperation creates AvailableOperationsOnKv instance for the given operation_query.
    /// Returns Syntax Error if query is incorrect
    /// ## Example
    /// ```
    /// use multithreaded_kv_store::AvailableOperationsOnKv;
    /// let prepared_operation = AvailableOperationOnKv::prepare_operation;
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
    /// ## Keywords
    /// Write, Read, Delete keywords are case-insensitive
    ///
    /// ## Keys and Values
    /// Keys i.e \<key\> and Values i.e \<value\> are case-sensitive
    ///
    /// i.e name, Name, NAme are not equal, therefore, 3 distinct keys
    ///
    pub fn prepare_operation(operation_query: &str) -> Result<Self, String> {
        // let operation_query_lowered = operation_query.to_lowercase();
        let query_list: Vec<&str> = operation_query.split(' ').collect();

        if let Some(op_from_query) = query_list.get(0) {
            let op: Op;
            let op_from_query_lowered = &op_from_query.to_lowercase()[..];

            match op_from_query_lowered {
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
                    op = Op::ReadOp(ReadKv {
                        key: query_list.get(1).expect("Delete Syntax Error").to_string(),
                    });
                }
                _ => {
                    return Err("Syntax Error:\nIllegal Query, Coundn't find any operation in query, please write query using correct syntax.".to_string());
                }
            }

            Ok(AvailableOperationsOnKv { op: Some(op) })
        } else {
            return Err("Syntax Error:\nIllegal Query, Coundn't find any operation in query, please write query using correct syntax.".to_string());
        }
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

/// ## StoreEngine
/// StoreEngine manages respective Project's KV Store.
///
/// StoreEngine creates a 'projects/project_name' folder consisting of it's WAL log and latest snapshot.
///
/// Maintains ThreadPool, WAL.log, snapshot.bin, recovery, etc.
///
/// # Example
/// ```
/// use multithreaded_kv_store::StoreEngine;
///
/// let my_kv_store_engine = StoreEngine::new("my_kv");
///
/// ```
#[derive(Debug)]
pub struct StoreEngine {
    project_name: String,

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
}

impl StoreEngine {
    /// connect() associative function connects to instance of StoreEngine for the given 'project_name'
    /// project is part of Store
    ///
    /// # Example
    /// ```
    /// let my_kv_store_engine = StoreEngine::new("my_kv");
    /// ```
    pub fn connect(project_name: &str) -> Self {
        let wal_folder_path = format!("projects/{project_name}/wal");

        if let Err(e) = create_dir_all(&wal_folder_path) {
            panic!(
                "Failed to ensure directory exists\nDirectory Path: {}\n{e}",
                &wal_folder_path
            );
        }

        let wal_file_path = wal_folder_path + "/wal.log";

        match File::options().read(true).write(true).open(&wal_file_path) {
            Ok(file) => {
                println!("Starting {project_name} KV Store's Recovery!");
                return StoreEngine::recovery_restart(project_name, file);
            }

            Err(e) if e.kind() == ErrorKind::NotFound => {
                println!("Creating {project_name} KV Store!!");
                return StoreEngine {
                    project_name: project_name.to_string(),
                    next_seq_no: 1, //This has to be a natural number i.e > 0, even 0 is not allowed
                    in_memory_storage: MemStorage {
                        sq_no: 0, //for now this value doesn't really matter
                        mem_storage: HashMap::new(),
                    },
                    wal_file: File::create_new(&wal_file_path).unwrap(),
                    snapshot_file: None,
                };
            }

            Err(e) => {
                panic!("Error Occured while creating wal.log file.\n{e}")
            }
        }
    }

    fn recovery_restart(project_name: &str, file: File) -> Self {
        //Recovery code goes here
        //Recovery means getting the in_memory_storage in the correct latest state using logs and snapshots

        return StoreEngine {
            project_name: project_name.to_string(),
            next_seq_no: 1, //This has to be a natural number i.e > 0, even 0 is not allowed
            in_memory_storage: MemStorage {
                sq_no: 0, //for now this value doesn't really matter
                mem_storage: HashMap::new(),
            },
            wal_file: file,
            snapshot_file: None,
        };
    }
}
