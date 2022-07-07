# Introduction
The cppsuite is a C++ framework designed to help developers write multithreaded and highly configurable testing for WiredTiger. It is intended to make stress testing as well as scenario-driven testing easy to implement. The framework provides built-in [components](#components) that each offers a set of features to enhance the testing experience and [database operations](#database-operations) with a default implementation that can be overridden at the test level. Each test has a corresponding [configuration file](#test-configuration-file) that defines the workload.

# Table of contents
- [Introduction](#introduction)
- [Database operations](#database-operations)
  * [Populate](#populate)
  * [Insert](#insert)
  * [Update](#update)
  * [Remove](#remove)
  * [Read](#read)
  * [Custom](#custom)
  * [Checkpoint](#checkpoint)
  * [Validate](#validate)
- [Components](#components)
  * [Workload Manager](#workload-manager)
  * [Operation Tracker](#operation-tracker)
  * [Timestamp Manager](#timestamp-manager)
  * [Metrics Monitor](#metrics-monitor)
- [Test configuration file](#test-configuration-file)
- [How to execute a test](#how-to-execute-a-test)

# Database operations
The database operations and their default implementation are described in the subsections below. During a test, a user-defined number of threads can be associated to each of those operations. Currently, the framework only supports operations on keys and values of type string. The values are randomly generated using a uniform distribution.

## Populate
Populate is the first operation called in a test, before any other operation. It fills the database with an initial dataset. The number of collections, keys per collection, size of keys and values are defined in the configuration file. For each collection, we insert the configured number of key/value pairs which are all unique. If more than one thread is configured for this operation, the framework divides the number of collections across the number of threads.

## Insert
An insert operation inserts a unique key with a random value into a collection. The number of insert threads, insertions performed in each transaction, sleep time between each insert, size of keys and values are defined in the configuration file. The insert threads are evenly divided between the number of collections created during the populate phase. Only one thread works on a collection at any given time.

## Update
An update operation uses a random cursor to select a key in the database and updates it with a random value. The number of update threads, updates performed in each transaction, sleep time between each update, size of keys and values are defined in the configuration file.

## Remove
A remove operation uses a random cursor to select and remove a key from the database. The number of remove threads, removals performed in each transaction and sleep time between each removal are defined in the configuration file.

## Read
A read operation selects a random collection and uses a random cursor to read items from it. The number of read threads, reads performed in each transaction and the sleep time between each read are defined in the configuration file.

## Custom
There is no default implementation as this function is intended to be user-defined. The number of threads, operations performed in each transaction, sleep time between each operation, size of the keys and values are defined in the configuration file. 

## Checkpoint
A checkpoint operation executes a checkpoint on the database every 60 seconds. Checkpoints are enabled or disabled by assigning one or zero threads in the configuration file, they are enabled by default. Checkpoints are executed periodically according to the configured operation rate.

## Validate
The default validation algorithm requires the default [operation tracker](#operation-tracker) configuration. If the operation tracker is reconfigured, the default validation is skipped. The default validator checks if the WiredTiger tables are consistent with the tables tracked by the operation tracker by comparing their content. The validation (and hence the test) fails if there is any mismatch.

# Components
The framework provides built-in components that each offer a set of features to enhance the testing experience. Their behaviour is customized through a configuration file.  A component has a life cycle made of three stages: *load*, *run*, and *finish*. Each of these stages is described for each component below.


## Workload Manager
The workload manager is responsible for calling the populate function and the lifecycle of the threads dedicated to each of the database operations described above.

| Phase    | Description |
| -------- | ----------- |
| Load     | N/A         |
| Run      | Calls the populate function, then parses the configuration for each operation and spawns threads for each of them. |
| Finish   | Ends each thread started in the run phase. |

## Operation Tracker
During the execution of the test, by default the operation tracker saves test metadata every time a thread performs an update, insertion, or removal operation. The user can also manually save test metadata by calling the `save_operation` function from the `operation_tracker` class. The framework defines a set of default data to track during a test but the user can customise it by editing the test configuration file and overriding the `set_tracking_cursor` function. See [here](HOWTO.md) for more details.
Any saved data can be used at the [validation](#validation) stage.

| Phase    | Description |
| -------- | ----------- |
| Load     | Creates two tables to save the test metadata. One (A) is dedicated to schema operations and the second (B) is dedicated to any other operation. |
| Run      | If the key/value format of table (B) is not overridden by the user, it prunes any data not applicable to the validation stage. |
| Finish   | N/A |

## Timestamp Manager
The timestamp manager is responsible for managing the stable and oldest timestamps as well as providing timestamps to the user on demand. The stable and oldest timestamps are updated according to the definitions of the `oldest_lag` and `stable_lag` in the test configuration file. The `oldest_lag` is the difference in time between the oldest and stable timestamps, and `stable_lag` is the difference in time between the stable timestamp and the current time. When the timestamp manager updates the different timestamps it is possible that concurrent operations may fail. For instance, if the oldest timestamp moves to a time more recent than a commit timestamp of a concurrent transaction, the transaction will fail. 

| Phase    | Description |
| -------- | ----------- |
| Load     | Initialises the oldest and stable lags using the values retrieved from the configuration file. |
| Run      | Updates the oldest and stable lags as well as the oldest and stable timestamps. |
| Finish   | N/A |

## Metrics Monitor
The metrics monitor polls database metrics during and after test execution. If a metric is found to be outside the acceptable range defined in the configuration file then the test will fail. When a test ends, the metrics monitor can export the value of these statistics to a JSON file that can be uploaded to Evergreen or Atlas.

| Phase    | Description |
| -------- | ----------- |
| Load     | Retrieves the metrics from the configuration file that need to be monitored during and after the test. |
| Run      | Checks runtime metrics and fails the test if one is found outside the range defined in the configuration file. |
| Finish   | Checks post run metrics and fails the test if one is found outside the range defined in the configuration file. Exports any required metrics to a JSON file if the test is successful. |

All the components and their implementation can be found in the [component](https://github.com/wiredtiger/wiredtiger/tree/develop/test/cppsuite/src/component) folder.

# Test configuration file
A test configuration is a text file that is associated with a test and which defines the workload. The test configuration file contains top-level settings such as the test duration or cache size and component level settings that define their behaviour. The format is the following:

```cpp
# Top level settings
duration_seconds=15,
cache_size_mb=200,
...,
# Components settings
timestamp_manager=
(
  enabled=true,
  oldest_lag=1,
  stable_lag=1,
  ...,
),
...,
```

Each test has a default configuration file which is the test name followed by the suffix `_default` and the extension `.txt`. It is possible to use any configuration file when executing a test, see [here](HOWTO.md) for more details.

All the different configurable items are defined in [test_data.py](https://github.com/wiredtiger/wiredtiger/blob/develop/dist/test_data.py).


# How to execute a test
A tutorial is available [here](HOWTO.md).