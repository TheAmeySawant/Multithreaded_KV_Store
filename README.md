# Multithreaded KV Store

This project is the first step toward a distributed key-value store.
It currently focuses on a local, multithreaded foundation: a Tokio-based server, a protobuf-defined operation model, and a write-ahead log backed storage engine.

## What It Does

- Starts a TCP server on `127.0.0.1:7878`
- Serves simple HTML responses from the `pages/` folder
- Creates and manages per-project storage under `projects/<project_name>/wal/`
- Uses a protobuf schema to model KV operations such as write, read, and delete
- Stores operations in a WAL so the project can grow into a recoverable storage engine

## Why This Project Exists

The goal is not just to build a single-node KV store.
The goal is to develop the storage, logging, and protocol pieces that can later be expanded into a distributed KV store.

This codebase is intended to become a foundation for:

- request routing across nodes
- replication
- recovery after crashes
- snapshotting
- eventually, distributed consistency and coordination

## Current Architecture

### Server

`src/main.rs` starts a multithreaded Tokio application and listens for incoming TCP connections.
Incoming requests are matched against a small set of routes such as:

- `GET /` for the home page
- `GET /create-kv` to create a KV project store
- `GET /sleep` to simulate a slow request
- everything else falls back to `404`

### Storage Engine

`src/lib.rs` contains `StoreEngine`, which is responsible for the local KV store lifecycle.
It creates a project directory, prepares a WAL file, and keeps an in-memory `HashMap` for data.

The design already anticipates future recovery behavior, even though full recovery logic is not implemented yet.

### Protobuf Protocol

`proto/kvstore.proto` defines the wire format for KV operations:

- write
- read
- delete
- WAL records containing a sequence number and operation payload

The build script compiles this schema at build time using vendored `protoc` so the project can build consistently on Windows.

## Project Structure

- `src/main.rs` - server entry point and request handling
- `src/lib.rs` - KV storage engine and protobuf-backed domain types
- `proto/kvstore.proto` - protobuf schema for KV operations and WAL records
- `build.rs` - generates Rust types from the protobuf schema
- `pages/` - static HTML pages used by the demo server
- `projects/` - runtime data for created KV projects

## Build

```bash
cargo build
```

The build script automatically compiles the protobuf definitions.

## Run

```bash
cargo run
```

Then open or request:

- `http://127.0.0.1:7878/`
- `http://127.0.0.1:7878/create-kv`
- `http://127.0.0.1:7878/sleep`

## Development Notes

- The project currently uses a local WAL and in-memory storage model.
- Recovery is planned but not complete.
- Some code is still experimental and the project should be treated as an evolving prototype rather than a finished database.

## Roadmap Toward Distributed KV Storage

1. Finish durable local recovery from WAL and snapshots.
2. Add structured client/server KV commands over the protobuf schema.
3. Introduce node-to-node communication.
4. Add replication and leader-based coordination.
5. Extend the design toward a distributed KV store with fault tolerance.

## License

No license has been specified yet.
