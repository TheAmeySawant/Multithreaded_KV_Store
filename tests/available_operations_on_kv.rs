
use multithreaded_kv_store::{StoreEngine, Op};


#[test]
fn prepare_write_operation() {
    let operation =
        StoreEngine::prepare_operation("Write name = Amey").unwrap();

    match operation.op.unwrap() {
        Op::WriteOp(write) => {
            assert_eq!(write.key, "name");
            assert_eq!(write.value, "Amey");
        }
        _ => panic!("Expected WriteOp"),
    }
}

#[test]
fn prepare_read_operation() {
    let operation =
        StoreEngine::prepare_operation("Read name").unwrap();

    match operation.op.unwrap() {
        Op::ReadOp(read) => {
            assert_eq!(read.key, "name");
        }
        _ => panic!("Expected ReadOp"),
    }
}

#[test]
fn prepare_delete_operation() {
    let operation =
        StoreEngine::prepare_operation("Delete name").unwrap();

    match operation.op.unwrap() {
        Op::DeleteOp(delete) => {
            assert_eq!(delete.key, "name");
        }
        _ => panic!("Expected DeleteOp"),
    }
}

#[test]
fn keyword_is_case_insensitive() {
    assert!(StoreEngine::prepare_operation("write name = Amey").is_ok());
    assert!(StoreEngine::prepare_operation("WRITE name = Amey").is_ok());
    assert!(StoreEngine::prepare_operation("WrItE name = Amey").is_ok());
}

#[test]
fn key_and_value_are_case_sensitive() {
    let operation =
        StoreEngine::prepare_operation("Write Name = Amey").unwrap();

    match operation.op.unwrap() {
        Op::WriteOp(write) => {
            assert_eq!(write.key, "Name");
            assert_eq!(write.value, "Amey");
        }
        _ => panic!("Expected WriteOp"),
    }
}

#[test]
fn invalid_write_syntax_returns_error() {
    assert!(StoreEngine::prepare_operation("Write name Amey").is_err());
}

#[test]
fn invalid_read_syntax_returns_error() {
    assert!(StoreEngine::prepare_operation("Read").is_err());
}

#[test]
fn invalid_delete_syntax_returns_error() {
    assert!(StoreEngine::prepare_operation("Delete").is_err());
}

#[test]
fn unknown_operation_returns_error() {
    assert!(StoreEngine::prepare_operation("Update name = Amey").is_err());
}

#[test]
fn empty_query_returns_error() {
    assert!(StoreEngine::prepare_operation("").is_err());
}