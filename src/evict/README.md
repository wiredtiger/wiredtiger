# Eviction

### Overview

Eviction process ensures that the cache in WiredTiger is efficiently managed and it stays within user-defined boundaries. These boundaries are set through **target<sup>1</sup>** and **trigger<sup>2</sup> thresholds** for total<sup>3</sup>, dirty<sup>4</sup>, and updates<sup>5</sup> content. When the cache exceeds these limits, eviction processes begins to evict the content in cache.

### Key Parameters

WiredTiger offers multiple configuration options to manage the eviction of pages from the cache. These settings can be adjusted during database opening (`wiredtiger_open`) or reconfigured later (`WT_CONNECTION::reconfigure`).

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

- **Eviction Server**: The primary background process that commences when the **target<sup>1</sup> thresholds** are reached. It identifies evictable candidates, places them in eviction queues and sorts them based on the **Least Recently Used (LRU)** algorithm.
- **Eviction Worker Threads**: These threads pop pages from the queues and evicts them. They run in the background to assist the server.
- **Application Threads**: These threads assist with eviction when **trigger<sup>2</sup> thresholds** are reached or may assist in evicting pages directly if needed (this is known as *forced eviction*).

> It is possible to run only the eviction server without worker threads, but this may result in slower eviction processes as the server will be also be responsible for evicting the pages from the queues.

### APIs for Eviction

The eviction APIs, defined in `evict.h`, allow other modules in WT to manage eviction processes. Below is a brief description of the functionalities provided by these APIs:

- Interrupting and waking up the eviction server when necessary.
- Specifying which files to prioritise or exclude from the eviction process.
- Retrieving the state of cache health from the eviction.
- Allowing external modules to participate in the eviction process, enabling them to evict individual pages or entire data trees if needed.
- Modifying page states, crucial for prioritising or de-prioritising pages for eviction.

### Terminology

- <sup>1</sup>target: The level of cache usage eviction tries to maintain.
- <sup>2</sup>trigger: The level of cache usage levels at which application threads assist with the eviction of pages.
- <sup>3</sup>total: The combined memory usage of clean pages, dirty pages, and in-memory updates.
- <sup>4</sup>dirty: The memory usage of all the modified pages that have not yet been written to disk.
- <sup>5</sup>updates: The memory usage of all the in-memory updates.
