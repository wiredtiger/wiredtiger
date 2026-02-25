# WiredTiger Node.js Bindings

This package provides a high-performance Node.js interface for the WiredTiger storage engine. The API is designed to mirror the [WiredTiger Python API](https://source.wiredtiger.com/12.0.0/python.html) while providing idiomatic JavaScript features like iterators.

## Prerequisites

- Node.js (v14 or later recommended)
- `node-gyp` installed globally (`npm install -g node-gyp`)
- A compiled version of the WiredTiger library (`libwiredtiger.dylib` or `.so`) in the project's `build` directory.

## Installation

1. Navigate to the `lang/nodejs` directory.
2. Install dependencies and build the native addon:

```bash
npm install
```

## Basic Usage

### Opening a Connection and Session

```javascript
const wt = require('./lib/index');

// Open a connection (create if it doesn't exist)
const conn = wt.open('WT_HOME', null, 'create');

// Open a session
const session = conn.open_session();
```

### Working with Tables

```javascript
// Create a table with String keys and values
session.create('table:mytable', 'key_format=S,value_format=S');

// Open a cursor
const cursor = session.open_cursor('table:mytable');

// Insert a record
cursor.set_key('key1');
cursor.set_value('value1');
cursor.insert();

// Search for a record
cursor.set_key('key1');
if (cursor.search() === 0) {
    console.log('Found:', cursor.get_value());
}

cursor.close();
```

### Iteration

The `Cursor` object supports the JavaScript iterator protocol, allowing you to use `for...of` loops:

```javascript
const cursor = session.open_cursor('table:mytable');

for (const [key, value] of cursor) {
    console.log(`${key}: ${value}`);
}

cursor.close();
```

### Transactions

```javascript
session.begin_transaction();
try {
    const cursor = session.open_cursor('table:mytable');
    cursor.set_key('key2');
    cursor.set_value('value2');
    cursor.insert();
    
    session.commit_transaction();
} catch (err) {
    session.rollback_transaction();
    console.error('Transaction failed, rolled back:', err);
}
```

## API Reference

### `wiredtiger` (Module)
- `open(home, err_handler, config)`: Opens a connection to the database.
- `version()`: Returns the WiredTiger version string.
- `strerror(error_code)`: Returns the string representation of an error code.
- `WT_NOTFOUND`: Constant for "item not found".

### `Connection`
- `open_session(config)`: Opens a new Session.
- `close(config)`: Closes the connection.

### `Session`
- `open_cursor(uri, to_dup, config)`: Opens a cursor on a table or data source.
- `create(name, config)`: Creates a table or other data source.
- `drop(name, config)`: Drops a data source.
- `begin_transaction(config)`: Starts a transaction.
- `commit_transaction(config)`: Commits the current transaction.
- `rollback_transaction(config)`: Rolls back the current transaction.
- `close(config)`: Closes the session.

### `Cursor`
- `set_key(key)`: Sets the key for the next operation (String or Buffer).
- `set_value(value)`: Sets the value for the next operation (String or Buffer).
- `get_key()`: Returns the current key.
- `get_value()`: Returns the current value.
- `next()` / `prev()`: Move the cursor.
- `search()`: Search for a record matching the current key.
- `insert()` / `update()` / `remove()`: Modify data.
- `reset()`: Reset the cursor position and clear the key/value.
- `close()`: Closes the cursor.

## Current Limitations

- **Schema Support**: Currently optimized for `S` (String) and `u` (Raw Bytes/Buffer) formats. Complex schemas (like `iS`) require manual packing using Buffers.
- **Async API**: All operations are currently synchronous to match the core WiredTiger C API behavior.

## Running Tests

```bash
node test/test.js
node test/test-txn.js
```
