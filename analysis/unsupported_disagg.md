# Unsupported WT features in disagg

## May 2026

This document maintains a list of unsupported WiredTiger features under the disaggregated storage architecture mode. While some of these features are not applicable for disagg and won’t be supported by design, others could be just lack of support temporarily with expectations the support will be added over time to achieve feature parity with the attached storage (classic) WT architecture mode.

Items marked as “Never” supported in disagg indicate that there is no plan to support the functionality up to GA. We can’t predict what might be needed or useful after GA.

| *WT feature* | *Currently supported in disagg?* | *When will be supported in disagg?* | *Reason / Context* |
| :---- | :---- | :---- | :---- |
| WAL  | No | Never | Writing the OpLog to the Log Service provides commit-level durability in disagg. So we don’t need to use WT’s write-ahead-log for this.  Having the WAL record the history of operations on table has been a useful diagnostic tool in the past for ASC. In DSC deployment we can use the WAL mechanism to record transactions on the local node, then be able to inspect those records as a way of reconstructing how/why a particular failure mode is being encountered. |
| Non-timestamped user tables | No  | Never | All tables created by mongod must be timestamped. Internal WT tables, such as the history store or metadata might not require timestamps |
| RTS | No | Never  | The server needs to rollback transactions in the event of an unplanned step down. But we will address this by rolling back to the previous checkpoint. |
| Precise checkpoint | Yes  | N/A | As no RTS is supported, internally WT leverages “precise checkpoint” to make sure only stable data is written to disk. |
| Fuzzy checkpoint | No | Never | Won’t be used in DSC deployment. |
| Checkpoint cursor | No |  ~~maybe~~?  Private Preview (potential) | [WT-15357](https://jira.mongodb.org/browse/WT-15357) could allow checkpoint=WiredTigerCheckpoint cursors.  The server team plans to start designing resharding “over next couple of months” which could make use of checkpoint cursor. Checkpoint cursor may also make testing easier.  |
| Prepared Txn | No (different and need new functionality in DSC) | Public Preview | This is different in DSC. There is new functionality \- which is to guarantee that prepared content is included in a checkpoint if it adheres to timestamp rules, whereas before it would sometimes include content in a checkpoint if there was cache pressure. |
| Compaction/ background Compaction | No | Never | Compaction optimizes data layout within local files in ASC.  Since DSC doesn’t store data in local files this functionality is not relevant for DSC deployments. [WT-15273](https://jira.mongodb.org/browse/WT-15273). |
| Fast Truncate | No | Public Preview | Used by oplog truncation and change streams |
| Slow Truncate | Yes | Private Preview | This is available\! |
| Salvage | No | ~~Never~~ No plan | *Maybe* there would be a salvage but it would be in a very different form from current salvage. [WT-14740](https://jira.mongodb.org/browse/WT-14740) |
| Column store | No | Never | Used by the ASC implementation of the encrypted storage engine. A different implementation strategy is being used for DSC. |
| Backup cursors | No | Never | Backup is taking a very different approach; it won’t use backup cursors |
| Table Import | No | No plan | Might be a use case for this someday (after GA) |
| In-memory db | No | No plan | Might be a use case for this someday (after GA) |
| WT\_SESSION::alter | No | No plan | Might be a use case for this someday (after GA).   Mongod uses alter for repair in ASC. |
| Modify::ops | Maybe (we have a failing test) | Public Preview | [WT-14467](https://jira.mongodb.org/browse/WT-14467) Probably should prioritize? |
| Verify | Yes | Additional testing may be added for Public Preview | We need to support this on a stable table. Refer to [SPM-4352](https://jira.mongodb.org/browse/SPM-4352) for details.  [Decision: Verify Scope For Layered Tables](https://docs.google.com/document/d/1GwWMx6flQs0j0nq_K5NOyCt5zya4n6VCKk2QlNfdZJM/edit?tab=t.0#heading=h.hgsymz6wyzzc) |
| Table drop | Yes | Private Preview | Ticket’s worth of work. Is there a ticket?  If needed, should prioritize. [WT-14503](https://jira.mongodb.org/browse/WT-14503) |
| Bulk load  | No | Not planned | [WT-14563](https://jira.mongodb.org/browse/WT-14563) Does mongod use this in ASC?  Bulk load is used when creating indexes on existing tables. It is an important performance optimization. |
| Read only mode | No | Maybe | The ASC server does not use read-only mode and there are no anticipated uses of it for DSC. Read-only mode \*is\* used by the wt utility. So we may want to support it for tools rather than for the server.  [WT-17143](https://jira.mongodb.org/browse/WT-17143)  |
| Prefetch | ??? | Not planned | This is required for server features, but we might consider using prefetch if we find compelling performance gains from enabling it |
| Custom collators | ??? | Not planned |  |
| Step-up with positioned cursors/ongoing transactions | Partially | Public preview | Stepping up is supported, but all cursors must be reset before stepping up. |
| Elegant step-down | Partially | Public preview | Currently step-down should be done by restarting a node. Elegant stepping now without a restart is planned for PuP |
| Named checkpoint | No | No plan | Not used by the server |


References:

* [SE Disagg Post-IB Planning](https://docs.google.com/spreadsheets/d/1zqmyhXH8370sIo79GV3Kx4kGSWDupW_dwkLALIvYgrw/edit?gid=0#gid=0)