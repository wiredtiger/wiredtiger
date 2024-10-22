# Eviction

### Overview

Eviction process ensures that the cache in WiredTiger is efficiently managed and it stays within user-defined boundaries. These boundaries are set through **target<sup>1</sup>** and **trigger<sup>2</sup> thresholds** for total<sup>3</sup>, dirty<sup>4</sup>, and updates<sup>5</sup> content. When the cache exceeds these limits, eviction processes begins to evict the content in cache.

### Key Parameters

WiredTiger offers multiple configuration options to manage the eviction of pages from the cache. These are defined in [api_data.py](../../dist/api_data.py). These settings can be adjusted during database opening (`wiredtiger_open`) or reconfigured later (`WT_CONNECTION::reconfigure`). The most important configuration options for eviction are:

| Parameter               | Description                                                                                              |
| ----------------------- | -------------------------------------------------------------------------------------------------------- |
| **`eviction_target`**    | The target<sup>1</sup> percentage of cache usage that eviction tries to maintain.                                  |
| **`eviction_trigger`**   | When cache usage exceeds this threshold, **application threads**  start assisting in eviction.     |
| **`eviction_dirty_target`** | The target<sup>1</sup> percentage of **dirty<sup>4</sup>** cache usage that eviction tries to maintain.                      |
| **`eviction_dirty_trigger`** | When cache usage exceeds this **dirty<sup>4</sup>** threshold, **application threads**  start assisting in eviction.   |
| **`eviction_updates_target`** | The target<sup>1</sup> percentage of **updates<sup>5</sup>** cache usage that eviction tries to maintain.                 |
| **`eviction_updates_trigger`** | When cache usage exceeds this **updates<sup>5</sup>** threshold, **application threads** start assisting in eviction.                   |

> **Note:** Target<sup>1</sup> sizes must always be lower than their corresponding trigger<sup>2</sup> sizes.

### Eviction Process

The eviction process involves three components:

- **Eviction Server**: The `eviction server` thread identifies evictable pages/candidates, places them in eviction queues and sorts them based on the **Least Recently Used (LRU)** algorithm. It is a background process that commences when the **target<sup>1</sup> thresholds** are reached.
- **Eviction Worker Threads**: These threads pop pages from the eviction queues and evicts them. The `threads_max` and `threads_min` configurations in [api_data.py](../../dist/api_data.py) control the maximum and minimum number of eviction worker threads in WiredTiger. They also run in the background to assist the server.
    > It is possible to run only the eviction server without the eviction worker threads, but this may result in slower eviction as the server thread alone would be responsible for evicting the pages from the eviction queues.
- **Application Threads Eviction**: 
    - When eviction threads are unable to maintain cache content, and cache content reaches **trigger<sup>2</sup> thresholds**, application threads begin assisting the eviction worker threads by also evicting pages from the eviction queues.
    - Another scenario, known as **forced eviction**, occurs when application threads directly evict pages in specific conditions, such as:
        - Pages exceeding the configured `memory_page_max` size (defined in [api_data.py](../../dist/api_data.py))
        - Pages with large skip list
        - Empty Internal pages
        - Obsolete pages
        - Pages with long update chains
        - Pages showing many deleted records
    > In both cases described above, application threads may experience higher read/write latencies.

### APIs for Eviction

The eviction APIs, defined in `evict.h`, allow other modules in WT to manage eviction processes. Below is a brief description of the functionalities provided by these APIs:

- Interrupting and waking up the eviction server when necessary.
- Specifying which Btrees to prioritise or exclude from the eviction process.
- Retrieving the state of cache health.
- Allow callers to evict pages or entire Btrees directly, bypassing the background eviction process.
- Modifying page states, crucial for prioritising or de-prioritising pages for eviction.

### Terminology

- <sup>1</sup>target: The level of cache usage eviction tries to maintain.
- <sup>2</sup>trigger: The level of cache usage at which application threads assist with the eviction of pages.
- <sup>3</sup>total: The combined memory usage of clean pages, dirty pages, and in-memory updates.
- <sup>4</sup>dirty: The memory usage of all the modified pages that have not yet been written to disk.
- <sup>5</sup>updates: The memory usage of all the in-memory updates.
