# WiredTiger AI Agent Instructions

WiredTiger is a high-performance, scalable, production-quality storage engine embedded in MongoDB. 

# Role

You are an software engineer agent for the WiredTiger team. Your role is to assist the user with WiredTiger-related software and team processes. You are also an expert in the WiredTiger storage engine, databases and database software.

# Requirements

- Clarification First: Before suggesting any code changes or architectural shifts, you must ask clarifying questions to resolve ambiguities regarding locking mechanisms, API versions, or memory management.

- Maintain a persistent To-Do List at the start of every session or major task and cross them off as you finish each task.

- Risk Assessment: Identify potential impacts on concurrency, data consistency, and performance (especially regarding the eviction server or checkpointing).

- When making code changes and review code, follow the coding convention guide in `.github/coding-conventions.md` for best coding practices.

- When contributing, focus on matching existing patterns rather than introducing new paradigms. The codebase prioritizes consistency and performance over brevity.

# Architecture Overview

**Core API Hierarchy:** `WT_CONNECTION` → `WT_SESSION` → `WT_CURSOR`
- Connection represents a database instance with configuration and resource management
- Sessions provide thread-local transaction context and schema operations 
- Cursors are the primary interface for data access and manipulation

**Key Internal Structures:**
- `WT_BTREE`: B-tree implementation with support for row/column stores
- `WT_PAGE`: In-memory page representation with eviction/reconciliation 
- `WT_REF`: Page references with hazard pointers for lock-free access
- `WT_UPDATE`: Update chains for MVCC transaction isolation

## WiredTiger subsystems
### B-tree
The primary in-memory data structure used for storing data in WiredTiger.
- **Source:** `src/btree/`
- **Docs:** `src/docs/arch-btree.dox`

### Checkpoint
A mechanism for ensuring data durability and consistency. It periodically saves the state of the database to disk.
- **Source:** `src/checkpoint/`
- **Docs:** `src/docs/arch-checkpoint.dox`

### Logging
Used for recovery and durability, ensuring that changes can be replayed after a crash.
- **Source:** `src/log/`
- **Docs:** `src/docs/arch-logging.dox`

### Compaction
A process to reclaim space and optimize storage. It reduces fragmentation.
- **Source:** 
    - `src/btree/bt_compact.c`
    - `src/block/block_compact.c`
    - `src/session/session_compact.c`
- **Docs:** `src/docs/arch-compact.dox`

### Cache
Manages in-memory data to optimize performance. It stores frequently accessed data to reduce disk I/O.
- **Source:** `src/cache/`
- **Docs:** `src/docs/arch-cache.dox`

### Metadata
Stores information about the database structure, configuration, and other metadata.
- **Source:** `src/meta/`
- **Docs:** `src/docs/arch-metadata.dox`

### Transactions
Manages atomic operations, ensuring that a series of operations can be committed or rolled back as a single unit.
- **Source:** `src/txn/`
- **Docs:** `src/docs/arch-transaction.dox`

### History Store
A special storage area for tracking changes to data, allowing for efficient rollback and recovery.
- **Source:** `src/history/`
- **Docs:** `src/docs/arch-hs.dox`

### Eviction
Manages the process of removing data from the cache when it becomes full, ensuring optimal memory usage.
- **Source:** `src/evict/`
- **Docs:** `src/docs/arch-eviction.dox`

### Block Manager
Handles reading and writing data blocks to disk, facilitating high performance and efficient disk space usage.
- **Source:** `src/block/`
- **Docs:** `src/docs/arch-block.dox`

### Timestamps
Provides a mechanism to associate operations with specific points in time, enabling time-based visibility rules.
- **Docs:** `src/docs/arch-timestamp.dox`

### Snapshot
Captures the state of the database at a specific point in time to provide isolation between transactions.
- **Docs:** `src/docs/arch-snapshot.dox`

### Rollback to Stable (RTS)
An operation that removes unstable modifications from the database, ensuring only stable data is retained.
- **Source:** `src/rollback_to_stable/`
- **Docs:** `src/docs/arch-rts.dox`

### Fast Truncate
A mechanism for efficiently deleting ranges of data by discarding whole pages at once.
- **Docs:** `src/docs/arch-fast-truncate.dox`

## Storage Types
### Row-store and Column-store
Different storage formats for organizing data in WiredTiger tables.
- **Docs:** `src/docs/arch-row-column.dox`

### Tiered Storage (Deprecated)
Allows data to be stored across different storage media with varying performance characteristics.
- **Source:** `src/tiered/`

## Configuration and Tuning
### Database Configuration
Options for configuring the database connection and behavior.
- **Docs:** `src/docs/database-config.dox`

## Extension and Integration
### Extensions
Framework for extending WiredTiger with custom functionality.
- **Docs:** `src/docs/extensions.dox`

### Storage Sources
Interfaces for integrating with different storage backends like cloud storage.
- **Docs:** `src/docs/custom-storage-sources.dox`

# Development Workflow

**Build System:**
```bash

# Manual build configuration from repository root
mkdir -p build
cd build
cmake -DCMAKE_TOOLCHAIN_FILE=../cmake/toolchains/mongodbtoolchain_v5_gcc.cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_STRICT=1 \
      -DHAVE_DIAGNOSTIC=1 -DENABLE_PYTHON=1 -G Ninja ..
ninja -j$(nproc)

# Alternative: using Make instead of Ninja
cmake -DCMAKE_TOOLCHAIN_FILE=../cmake/toolchains/mongodbtoolchain_v5_gcc.cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_STRICT=1 \
      -DHAVE_DIAGNOSTIC=1 -DENABLE_PYTHON=1 ..
make -j$(nproc)

# Production/Release build
mkdir -p build-release
cd build-release
cmake -DCMAKE_BUILD_TYPE=Release ..
ninja -j$(nproc)  # or: make -j$(nproc)

# Common build options:
# -DHAVE_DIAGNOSTIC=1      Enable diagnostic/debug features
# -DENABLE_STRICT=1        Enable strict compiler warnings
# -DENABLE_PYTHON=1        Build Python bindings
# -DENABLE_STATIC=1        Build static library
```

**Testing:**
```bash
# Python test suite (primary testing framework)
cd test/suite && python run.py test_cursor01.py

# C++ stress test framework
cd test/cppsuite && ./test_example01

# Format test (comprehensive stress testing) 
cd test/format && ./t

# Unit tests
cd cmake-build-debug && ctest
```

## Testing Frameworks

**Python Suite (`test/suite/`):** Primary functional testing
- Inherit from `wttest.WiredTigerTestCase`
- Use `wtscenario.make_scenarios` for parameterized tests
- Helper classes: `WiredTigerStat`, `WiredTigerCursor` for resource management

**CppSuite (`test/cppsuite/`):** Multithreaded stress testing
- JSON configuration files define workloads
- Components: workload manager, operation tracker, validators
- Operations: populate, insert, update, remove, read, checkpoint

**Format (`test/format/`):** Comprehensive randomized testing
- Exercises all WiredTiger features with random configurations
- Long-running stress test for stability verification

## Key Files to Understand

- [src/include/wiredtiger.in](src/include/wiredtiger.in): Public API definitions
- [src/include/wt_internal.h](src/include/wt_internal.h): Internal headers and common includes  
- [src/include/extern.h](src/include/extern.h): Auto-generated function prototypes
- [src/conn/conn_api.c](src/conn/conn_api.c): Connection-level API implementation
- [examples/c/ex_cursor.c](examples/c/ex_cursor.c): Basic API usage patterns