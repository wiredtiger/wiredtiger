# New Feature	Required
We can use wiredtiger cursor to dump some key/value entries between a special key range.
However, these data from a special range couldn't ingest into another wiretiger instance directly. 
# Requirement Source
## BackGround
We are working on a strong consistent distributed k/v store named elasticell.
https://github.com/deepfabric/elasticell
We use Multi-raft to achive strong consistency between different nodes.
In a single node we must partition the key spaces into different key ranges with no overlap.
Same key ranges among different node construct a raft group, we say it a cell(or a data shard).
Cells maybe split merge or move from one node to another.
When a cell join into a new node, a few k/v entries between a special range should be inserted into the new node.
In the perspect of storage engine, some k/v record sorted by key in a special ranges should be ingest into another db instance. 

## Wiretiger Current State
Wiretiger support incremental log apply to another wiretiger db instance.
These logs come from a consecutive time range.
It's a tradition data replicate fasion like rdbms such as oracle,mysql and so on.

## General Purpose
In a distributed newsql/nosql database, node rebalance between data shard is key feature. If range sharding strategy is selected, range data moving is inevitable.
For instance, Cockroachdb/TiDB use rocksdb as local storage engine. rocksdb support ingest api from exnternal data source. They use the feature to move data sharding to a new node.

# Constrast
We use rocksdb as our storage engine first. 
However, there are lost of write stalls when we do a pure write test case with high pressure.
The write tps of rocksdb slowed down as 1% ast the stable state.The poor state may last more than 1 minute.
We found level 0 sst files accumulated more than the threshold of slow wdown trigger by tracking the rocksdb log.
After doing some research on rocksdb document, we found the write stall phenomenom is inevitalbe. 
If the db write speed is faster than level0 compaction speed for a enough long time, write stall occures in the end.
Rocksdb's level0 compaction couldn't scale to multi thread. 
We can descreases the slow down trigger Which means the write stall occures more frequently.
Then we do some benchmark on lots of storage engine such as wiredtiger,rocksdb,tokudb. 
Wiredtiger gives the most stablbe performance and a perfect write tps.
If wiredtiger supports range dump and ingest, we will be very happy to switch from rocks to wiredtiger.
