# DataFusion ORC Extension

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.73%2B-orange.svg)](https://www.rust-lang.org/)

A DataFusion extension providing ORC (Optimized Row Columnar) file format support for Apache DataFusion.

## Overview

`datafusion-orc-extension` is an independent extension project that adds comprehensive ORC file format support to the DataFusion query engine. Built on top of [orc-rust](https://github.com/datafusion-contrib/orc-rust), it provides functionality similar to DataFusion's Parquet support.

## Status

🚧 **Work in Progress** - Phase 4c (Predicate Pushdown) completed. Currently working on Phase 4a: Basic Reading Functionality (unit tests & docs in progress).

## Features

### Planned Features

**Implemented:**
- ✅ **Schema Inference**: Automatically infer table schema from ORC files
- ✅ **Statistics Extraction**: Extract file statistics (row count, file size) for query optimization
- ✅ **Metadata Reading**: Read ORC file metadata from ObjectStore
- ✅ **Multi-file Schema Merging**: Merge schemas from multiple ORC files
- ✅ **Predicate Pushdown**: Filter data at stripe and row level using ORC row indexes

**In Progress:**
- 🚧 **Basic Reading Testing**: Integration tests in place for schema inference, streaming, projection+LIMIT; unit/error-path coverage pending
- 🚧 **Error Handling**: Comprehensive error handling and edge cases

**Planned:**
- ⏳ **Column Projection Optimization**: Enhanced projection support for nested columns
- ⏳ **Write Support**: Write query results to ORC format

## Implementation Plan

### Phase 1: Project Setup ✅

- [x] Project structure initialization
- [x] Implementation plan documentation
- [x] Configure Cargo.toml dependencies
- [x] Create module structure

### Phase 2: Core FileFormat Implementation ✅

- [x] Implement `OrcFormatFactory`
  - [x] Implement `FileFormatFactory` trait
  - [x] Support configuration options
- [x] Implement `OrcFormat`
  - [x] Implement `FileFormat` trait
  - [x] Schema inference (`infer_schema`)
  - [x] Statistics extraction (`infer_stats`)
  - [x] Physical plan creation (`create_physical_plan`)
  - [x] File source creation (`file_source`)
- [x] Create ObjectStore to ChunkReader adapter
  - [x] `ObjectStoreChunkReader` implementation
  - [x] Async metadata reading support

### Phase 3: FileSource Implementation ✅

- [x] Implement `OrcSource`
  - [x] Implement `FileSource` trait
  - [x] Basic statistics support
- [x] Implement `OrcOpener`
  - [x] Implement `FileOpener` trait
  - [x] Async file reading
  - [x] RecordBatch stream generation
  - [x] Limit support (SQL LIMIT clause)
  - [x] File-level column projection pushdown (uses ProjectionMask to push down column selection to ORC reader)

### Phase 4a: Basic Reading Functionality (In Progress)

- [x] Core reading infrastructure
  - [x] ObjectStore integration
  - [x] ORC file parsing and RecordBatch generation
  - [x] Async stream reading
- [x] Schema inference
  - [x] ORC schema → Arrow schema conversion
  - [x] Multi-file schema merging
- [x] Basic statistics extraction
  - [x] File-level statistics (row count, file size)
- [x] Testing and validation
  - [x] Integration tests with sample ORC files (schema inference + record batch validation, projection + LIMIT checks, map/list coverage)
  - [x] Schema inference tests (basic types and complex types)
  - [ ] Unit tests for basic reading
  - [ ] Error handling tests
- [ ] Documentation
  - [ ] Basic usage examples
  - [ ] API documentation

### Phase 4b: Column Projection Optimization (In Progress)

- [x] Enhanced projection support
  - [x] Optimize ProjectionMask usage
  - [x] Support for nested column projection ordering
  - [ ] Performance testing and optimization

### Phase 4c: Predicate Pushdown ✅

- [x] Predicate conversion module
  - [x] Convert DataFusion PhysicalExpr to orc-rust Predicate
  - [x] Support comparison operators (=, !=, <, <=, >, >=)
  - [x] Support logical operators (AND, OR, NOT)
  - [x] Support IS NULL / IS NOT NULL predicates
- [x] Stripe-level filtering
  - [x] Use ORC row index statistics for filtering
  - [x] Skip filtered row groups via orc-rust with_predicate()
- [x] Integration with DataFusion
  - [x] OrcSource filter() method returns stored predicate
  - [x] OrcOpener applies predicate to ArrowReaderBuilder
- [x] Integration tests
  - [x] Tests for equality, comparison, compound, and null predicates

### Phase 5: Writing Functionality (Optional)

- [ ] Basic writing
  - Arrow RecordBatch → ORC file
  - ObjectStore integration
- [ ] Write configuration
  - Compression options (snappy, zlib, lz4, zstd, etc.)
  - Stripe size configuration
  - Row index stride configuration

### Phase 6: Testing and Documentation

- [ ] Unit tests
  - Schema inference tests
  - Reading functionality tests
  - Error handling tests
- [ ] Integration tests
  - End-to-end reading tests
  - SQL query tests
- [ ] Example code
  - Basic usage examples
  - SQL query examples
- [ ] API documentation

## Project Structure

```
datafusion-orc-extension/
├── Cargo.toml              # Project configuration and dependencies
├── README.md               # Project documentation
├── IMPLEMENTATION_PLAN.md  # Detailed implementation plan
├── src/
│   ├── lib.rs              # Library entry point
│   ├── file_format.rs      # FileFormat and FileFormatFactory implementation
│   ├── source.rs           # FileSource implementation
│   ├── reader.rs           # ORC file reading logic
│   ├── writer.rs           # ORC file writing logic (optional)
│   ├── metadata.rs         # ORC metadata processing
│   ├── opener.rs           # File opening and configuration logic
│   └── predicate.rs        # Predicate conversion (DataFusion → orc-rust)
└── tests/                  # Test files
    ├── basic_reading.rs    # Basic reading integration tests
    └── predicate_pushdown.rs # Predicate pushdown integration tests
```

## Dependencies

### Core Dependencies

- **datafusion-common**: DataFusion common functionality
- **datafusion-datasource**: DataFusion datasource abstractions
- **datafusion-execution**: DataFusion execution engine
- **datafusion-physical-plan**: DataFusion physical plan
- **datafusion-session**: DataFusion session management
- **orc-rust**: Rust implementation of ORC file format
- **arrow**: Apache Arrow in-memory format
- **object_store**: Object storage abstraction layer

### Development Dependencies

- **tokio**: Async runtime
- **futures**: Async programming utilities
- **async-trait**: Async trait support

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datafusion-orc-extension = { path = "../datafusion-orc-extension" }
```

Or from git (when available):

```toml
[dependencies]
datafusion-orc-extension = { git = "https://github.com/your-org/datafusion-orc-extension" }
```

## Usage

### Basic Example

```rust
use datafusion_orc_extension::OrcFormatFactory;
use datafusion::prelude::*;

// Create SessionContext
let ctx = SessionContext::new();

// Register ORC format
let orc_factory = OrcFormatFactory::new();
ctx.register_file_format("orc", orc_factory);

// Read ORC file
ctx.register_table(
    "my_table",
    ctx.read_table("file:///path/to/file.orc")?
)?;

// Execute query
let df = ctx.sql("SELECT * FROM my_table WHERE column > 100").await?;
df.show().await?;
```

> **Note**: Core reading functionality is implemented. Currently in Phase 4a: testing and validation. The API may change during development.

## Architecture

### Core Components

1. **OrcFormat**: Implements `FileFormat` trait, provides file format abstraction
2. **OrcFormatFactory**: Implements `FileFormatFactory` trait, creates format instances
3. **OrcSource**: Implements `FileSource` trait, provides datasource functionality
4. **OrcOpener**: Implements `FileOpener` trait, handles file opening and data streams

### Data Flow

```
SQL Query
    ↓
DataFusion Logical Plan
    ↓
DataFusion Physical Plan
    ↓
OrcFormat.create_physical_plan()
    ↓
DataSourceExec (using OrcSource)
    ↓
OrcOpener.open()
    ↓
orc-rust ArrowReader
    ↓
Arrow RecordBatch Stream
```

## Development

### Requirements

- Rust 1.73+ (matching orc-rust requirements)
- DataFusion 51.0.0+
- orc-rust 0.7.1+ (from crates.io)

### Building

```bash
cargo build
```

### Testing

```bash
cargo test
```

### Code Style

- Follow Rust official code style
- Format code with `cargo fmt`
- Check code quality with `cargo clippy`

## Contributing

Contributions are welcome! Please see [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) for detailed implementation plans.

### How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Roadmap

- [x] **v0.1.0**: Core infrastructure (FileFormat, FileSource, FileOpener) ✅
- [x] **v0.2.0**: Schema inference and statistics ✅
- [ ] **v0.3.0**: Basic reading functionality (Phase 4a) - Testing and validation ✅
- [ ] **v0.4.0**: Column projection optimization (Phase 4b) ✅
- [x] **v0.5.0**: Predicate pushdown support (Phase 4c) ✅
- [ ] **v0.6.0**: Writing functionality (Phase 5)
- [ ] **v1.0.0**: Production-ready version

## Related Projects

- [Apache DataFusion](https://github.com/apache/datafusion) - Query engine core
- [orc-rust](https://github.com/datafusion-contrib/orc-rust) - ORC file format Rust implementation
- [Apache Arrow](https://arrow.apache.org/) - Columnar in-memory format

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built on top of the excellent [orc-rust](https://github.com/datafusion-contrib/orc-rust) library
- Inspired by DataFusion's Parquet implementation
- Part of the Apache DataFusion ecosystem

---

**Note**: This project is currently in early development. APIs may change.
