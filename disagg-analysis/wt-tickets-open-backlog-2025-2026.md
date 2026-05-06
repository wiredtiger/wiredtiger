# WT Open/Backlog Tickets Created in 2025–2026

**Generated:** 2026-05-06  
**Status filter:** Open, Backlog  
**Date range:** 2025-01-01 – 2026-05-06  
**Total unique tickets:** 987  
**DisAgg-related tickets:** 389  
**Other tickets:** 598  

> **Note:** December 2025 may have up to ~15 missing tickets due to a 50-result pagination
> limit on a single broad query. All other months were fully covered with date-range splitting.

## Classification Criteria

A ticket is marked **DisAgg-related** if it matches any of:

- **Labels:** any of `Disag_*`, `WT_disagg_*`, `disaggregated-storage`, `disagg`,
  `Disagg`, `lc_bulk_04_29_26`, `layered-cursor`, `disagg-performance*`,
  `disag_perf_*`, `Disag_grouping_*`
- **Summary keywords:** `disagg/layered`, `PALI/PALite`, `page delta/page_log`,
  `ingest table/btree`, `stable table`, `shared metadata/disk/cache/storage`,
  `step-up/step-down`, `follower/leader`, `standby`, `precise checkpoint`,
  `checkpoint pick-up`, `fake checkpoint`, `version cursor`, `turtle file`,
  `stable schema epoch`, `cross checkpoint`, `[ds-XX.XX]` story pattern,
  `build/write delta`, `delta generation`

> **Note on `dc` label:** The `dc` label (appearing with `na-mdb` on some early 2025
> build failures) was *excluded* from DisAgg indicators — it is not reliably disagg-specific.

---

## DisAgg-Related Tickets (389)

| Key | Summary | Status | Type | Priority | Labels |
|-----|---------|--------|------|----------|--------|
| **WT-14100** | cache_bytes_dirty stat reported as zero | Backlog | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-14232** | Make many-collection test use private mongo repo | Open | Task | Major - P3 | diagnostics, lc_bulk_04_29_26 |
| **WT-14342** | Make table deletion a constant-time operation. | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-14361** | failed: unit-test-extra-long test_truncate16 on ubuntu2004 [wiredtiger @ dc07f172] | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-14408** | [ds-04.02][Storage Engines (Core)] Pre-mortems on durability (data corruption) | Open | Story | Major - P3 | Disag_Must_Have, Disag_Operations, Disag_Private_Preview, Disag_Storage, Disag_grouping_Operational_Readiness +2 |
| **WT-14413** | [ds-05.08][Storage Engines (Core)] Restore with RTO < 15 mins | Open | Story | Major - P3 | Disag_Customer, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Backup_Restore +2 |
| **WT-14415** | [ds-06.08][Storage Engines (Core)] Change stream support | Open | Story | Major - P3 | Disag_Customer, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Functional_Parity +4 |
| **WT-14416** | [ds-06.09][Storage Engines (Core)] Existing functional tests (Atlas and mongod) pass when compared to local storage clusters | Open | Story | Major - P3 | Disag_Customer, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Functional_Parity +4 |
| **WT-14420** | [ds-07.04][Storage Engines (Core)] MongoD stores intermediate key for decrypting data encryption keys in SLS | Open | Story | Major - P3 | Disag_Customer, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Security +2 |
| **WT-14423** | [ds-08.06][Storage Engines (Core)] Local development environment for mongod/wt for unified binary | Open | Story | Major - P3 | Disag_Internal, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Release +2 |
| **WT-14427** | [ds-09.04][Storage Engines (Core)] 100% hygiene plan execution | Open | Story | Major - P3 | Disag_Internal, Disag_Must_Have, Disag_Public_Preview, Disag_Storage, Disag_grouping_Hygiene +3 |
| **WT-14429** | [ds-12.01][Storage Engines (Core)] Automated development environments (local and shared env) covering mongo/WT/storage layer for disaggregated storage | Open | Story | Major - P3 | Disag_Internal, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Dev_Experience +3 |
| **WT-14432** | [ds-14.03][Storage Engines (Core)] Performance benchmarking of various hardware/pod specifications within Atlas | Open | Story | Major - P3 | Disag_Internal, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Performance +2 |
| **WT-14433** | [ds-14.04][Storage Engines (Core)] Read performance matching NVME via a local cache | Open | Story | Major - P3 | Disag_Customer, Disag_Must_Have, Disag_Post_GA, Disag_Storage, Disag_grouping_Performance +3 |
| **WT-14434** | [ds-14.05][Storage Engines (Core)] Achieve performance parity with latest MongoD | Open | Story | Major - P3 | Disag_Internal, Disag_Launch, Disag_Must_Have, Disag_Storage, Disag_grouping_Performance +3 |
| **WT-14435** | [ds-14.06][Storage Engines (Core)] Automated Performance Regression Tests | Open | Story | Major - P3 | Disag_Internal, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Performance +2 |
| **WT-14436** | [ds-14.07][Storage Engines (Core)] High Value Workload (non YCSB) performance testing | Open | Story | Major - P3 | Disag_Internal, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Performance +2 |
| **WT-14440** | [ds-19.01][Storage Engines (Core)] Automatic recovery testing for mongod & SLS components from process, HW, or networking failures | Open | Story | Major - P3 | Disag_Customer, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Load_Resilience +3 |
| **WT-14441** | [ds-21.01][Storage Engines (Core)] Complete Durability threat model of SLS with mongod | Open | Story | Major - P3 | Disag_Internal, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Durability +2 |
| **WT-14442** | [ds-28.02][Storage Engines (Core)] Mongod admission control using SLS metrics | Open | Story | Major - P3 | Disag_Internal, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Multi-tenancy_Protection +4 |
| **WT-14454** | [ds-04.03][Storage Engines (Core)] Pre-mortems on availability | Open | Story | Major - P3 | Disag_Must_Have, Disag_Operations, Disag_Private_Preview, Disag_Storage, Disag_grouping_Operational_Readiness +1 |
| **WT-14463** | [ds-19.06][Storage Engines (Core)] Complete Availability threat model of SLS with mongod | Open | Story | Major - P3 | Disag_Internal, Disag_Must_Have, Disag_Private_Preview, Disag_Storage, Disag_grouping_Load_Resilience +1 |
| **WT-14469** | Fix missing stats on disagg block read path | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14470** | Block manager statistics need to account for deltas | Open | Bug | Major - P3 | Disag_Storage |
| **WT-14476** | WT shutdown fail | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14480** | Avoid evicting pages from WiredTiger metadata | Backlog | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14491** | Coordinate table drops across secondaries | Backlog | Story | Major - P3 | Disag_Storage |
| **WT-14492** | Review layered cursor implementation for optimization opportunities | Open | Task | Major - P3 | Disag_Storage |
| **WT-14493** | Review WiredTiger eviction behavior for checkpoint | Open | Task | Major - P3 | Disag_Storage |
| **WT-14494** | Use dhandle flag instead of dhandle name to identify history store | Open | Task | Major - P3 | Disag_Storage |
| **WT-14495** | Make tableId as uint32_t | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14496** | Overall design for distributed transaction commit in disaggregated storage | Open | Task | Major - P3 | Disag_Storage |
| **WT-14497** | Better define behaviour of precise checkpoint when no stable timestamp is set | Open | Task | Major - P3 | Disag_Storage |
| **WT-14501** | Investigate if we can reenable __btree_preload in __wt_btree_open | Open | Task | Major - P3 | Disag_Storage |
| **WT-14503** | Generate phylog entry when a table is dropped in WiredTiger | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14504** | Avoid writing duplicate full page to PALI | Open | Task | Major - P3 | Disag_Storage |
| **WT-14505** | Fix YCSB load phase hang on conclusion | Open | Task | Major - P3 | Disag_Storage |
| **WT-14507** | Extend wiredtiger cursor bound testing for layered tables | Open | Task | Major - P3 | Disag_Storage |
| **WT-14510** | Figure out overflow item usage | Open | Task | Major - P3 | Disag_Storage, WT_disagg_TBD, lc_bulk_04_29_26 |
| **WT-14512** | Review block metadata data structures in reconciliation | Open | Task | Major - P3 | Disag_Storage |
| **WT-14516** | Review WiredTiger reconciliation statistics for accuracy | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14520** | Layered table writes must use a timestamp | Open | Task | Major - P3 | Disag_Storage |
| **WT-14521** | Investigate if we need to consider transaction ids when garbage collecting from the ingest table | Open | Task | Major - P3 | Disag_Storage |
| **WT-14523** | Check fast truncate performance with disaggregated storage | Open | Task | Major - P3 | Disag_Storage, WT_disagg_TBD |
| **WT-14528** | Add a size threshold for we will always write a full page instead of a delta | Open | Task | Major - P3 | Disag_Storage |
| **WT-14529** | Fix or triage test failures currently ignored by the disagg hook PR testing | Open | Improvement | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14531** | Add histogram statistics to track the sizes of WT reads and writes | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14534** | Checkpoint should identify huge pages that cannot be split in memory and queue them for eviction | Open | Improvement | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14537** | Add WT stat to indicate whether WT is in leader or follower mode | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14540** | __wt_clayered_deleted should be inside the layered table module | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14541** | Extend WT disagg hook coverage to include followers | Open | Improvement | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14543** | Enhance layered cursor testing with oplog emulation. | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14544** | Decide how mongod server should handle unresponsiveness from SLS | Open | Improvement | Major - P3 | Disag_Storage, WT_disagg_design |
| **WT-14545** | Make layered cursors work with step-down | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14548** | Validate basic failover works for MDB/WT | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14549** | Bucketed stats for reconciliation page sizes | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14550** | fsync taking a meaningful proportion of checkpoint time | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14555** | Review and correct how page deltas are accounted for in read/write stats | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14559** | Investigate the number of eviction threads setting for disaggregated storage | Backlog | Task | Major - P3 | disag_perf_in_cache_100_update, disaggregated-storage, lc_bulk_04_29_26, performance |
| **WT-14563** | Support bulk load for layered cursors | Open | Bug | Major - P3 |  |
| **WT-14582** | Add support for readonly connections for disagg | Backlog | Bug | Minor - P4 | lc_bulk_04_29_26 |
| **WT-14583** | Investigate the size of index for SLS in YCSB out_of_cache workloads | Open | Task | Major - P3 | Disag_Storage, disaggregated-storage, lc_bulk_04_29_26 |
| **WT-14591** | Remove deprecated interfaces from PALI | Open | Task | Minor - P4 | Disag_Storage |
| **WT-14592** | Ensure PALI compilation fails with a sensible error message on Windows | Backlog | Task | Minor - P4 | Disag_Storage |
| **WT-14600** | Block manager size method assumes physical file | Open | Task | Major - P3 | Disag_Storage |
| **WT-14601** | Tune background eviction activity based on workload | Open | Improvement | Major - P3 | Disag_Storage, disag_perf_in_cache_100_update, performance |
| **WT-14608** | Block cache not abstracting deltas adequately | Open | Technical Debt | Minor - P4 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14612** | Investigate if __btree_preload should be used for disaggregated storage | Backlog | Technical Debt | Minor - P4 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14620** | Consider a structure for packed modify entries | Backlog | Improvement | Minor - P4 | Disag_Storage |
| **WT-14629** | Check if duplicate names are permitted in extension APIs | Backlog | Task | Minor - P4 | Disag_Storage |
| **WT-14630** | Understand eviction constraints in load workload with precise checkpoints | Open | Improvement | Major - P3 | Disag_Storage, disag_perf_128_thread_load, disagg-performance-investigation, performance |
| **WT-14642** | Move block manager interface out of block cache | Open | Improvement | Minor - P4 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14644** | Use extensible address cookies for disaggregated storage and beyond | Open | Improvement | Major - P3 | disaggregated-storage, lc_bulk_04_29_26, wt_data_format |
| **WT-14658** | Recommend default page sizes for disaggregated storage configurations | Open | Task | Major - P3 | Disag_Storage |
| **WT-14664** | [ds-09.05][Storage Engines] Design Review + Document for Layered Tables | Open | Story | Major - P3 | Disag_Storage, Storage_engines_outcome |
| **WT-14713** | test_import12 fails with failed to read 512 bytes at offset 7168 error | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-14716** | Compressor interface marks input argument as non-const | Open | Task | Minor - P4 | Disag_Storage |
| **WT-14717** | Check block_meta population when block cache is in use | Open | Task | Minor - P4 | Disag_Storage |
| **WT-14720** | Statistic for cache eviction ahead of materialisation frontier is misleading | Backlog | Task | Minor - P4 | Disag_Storage |
| **WT-14721** | Clean up disaggregated storage config | Open | Task | Minor - P4 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14722** | Clarify status of overflow items in disaggregated storage | Backlog | Task | Minor - P4 | Disag_Storage |
| **WT-14723** | Clarify status of table import in disaggregated storage | Open | Task | Minor - P4 | Disag_Storage |
| **WT-14725** | Performance improvements for shared checkpoints | Open | Task | Minor - P4 | Disag_Storage |
| **WT-14728** | Layered table creation string defined in multiple spots | Backlog | Task | Minor - P4 | Disag_Storage |
| **WT-14730** | Add some assertions when picking up new checkpoints | Open | Task | Minor - P4 | Disag_Storage |
| **WT-14732** | Improvements when copying ingest table content | Backlog | Task | Minor - P4 | Disag_Storage |
| **WT-14733** | Handle the case that passes a checkpoint_meta config to primary | Backlog | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14734** | Layered table lock potentially not being held long enough | Backlog | Task | Major - P3 | Disag_Storage |
| **WT-14735** | Layered tables performance improvements | Backlog | Task | Minor - P4 | Disag_Storage |
| **WT-14736** | Layered random cursors ignoring size of ingest table | Open | Task | Minor - P4 | Disag_Storage |
| **WT-14738** | Collators for layered tables | Backlog | Task | Minor - P4 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14739** | Secondary not performing shutdown checkpoint | Open | Task | Minor - P4 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14740** | Clarify how salvage works in disaggregated storage | Backlog | Task | Minor - P4 | Disag_Storage |
| **WT-14744** | Upgradable structure packing format | Open | Technical Debt | Major - P3 | lc_bulk_04_29_26, wt_data_format |
| **WT-14772** | Add comments about args in PALI functions | Open | Technical Debt | Major - P3 | Disag_Storage |
| **WT-14779** | hello_with_standby.js Segmentation fault __wt_evict_file during shutdown on standby | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-14788** | Disagg python testing: triage test_util*.py tests | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14789** | Usability improvements for CURSOR_API_CALL macro | Backlog | Task | Major - P3 | Disag_Storage |
| **WT-14794** | Implement caching of deltas on the write path | Open | Improvement | Minor - P4 | Disag_Storage |
| **WT-14795** | Add block cache testing | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14796** | Replace libmemkind for block cache | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14797** | Build a file-backed block cache | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14798** | Stop writing checkpointStart records to phylog | Open | Improvement | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14806** | Layered cursors tombstone ambiguity | Open | Task | Major - P3 | Disag_Storage |
| **WT-14808** | Add a block cache key extractor to the block manager | Open | Task | Major - P3 | Disag_Storage |
| **WT-14830** | Add stress testing to ensure prepared atomicity | Open | Task | Major - P3 | Disag_Storage |
| **WT-14860** | crash in __conn_reconfigure/__config_merge_scan | Backlog | Build Failure | Major - P3 | disaggregated-storage, lc_bulk_04_29_26 |
| **WT-14873** | Add fine-grained latency metrics for PALI reads | Open | Improvement | Major - P3 | Disag_Storage |
| **WT-14879** | Support generating page deltas for fast truncates | Backlog | New Feature | Minor - P4 | Disag_Storage |
| **WT-14881** | Improve pessimistic buffer size estimation when reconciling deltas | Open | Improvement | Minor - P4 | Disag_Storage |
| **WT-14882** | Revise rec_set_updates_durable usage | Open | Improvement | Minor - P4 | Disag_Storage |
| **WT-14883** | Unnecessary assignment to page->rec_lsn_max | Backlog | Improvement | Minor - P4 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14884** | Check if SLS needs an empty root page during checkpoint | Open | Task | Minor - P4 | Disag_Storage |
| **WT-14887** | Reconciliation key/value copy interface not expressive enough | Open | Improvement | Minor - P4 | Disag_Storage |
| **WT-14895** | Investigate __clayered_lookup and __clayered_put behaviour | Backlog | Improvement | Major - P3 | Disag_Storage, disagg-performance, lc_bulk_04_29_26 |
| **WT-14902** | Ensure RTS is run before allowing precise checkpoint reconfiguration | Backlog | Task | Major - P3 | Disag_Storage |
| **WT-14906** | [ds-06.05][Storage Engines (Core)] Multi-document transactions | Open | Story | Major - P3 | Disag_Customer, Disag_M12, Disag_Must_Have, Disag_Public_Preview, Disag_Storage +4 |
| **WT-14913** | Implement coherence vefication for ingest and stable tables | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-14915** | Extend DisAgg verification from the perspective of other components | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-14916** | prune_timestamp is only updated when adopting a new checkpoint | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14938** | Some layered tests not working for tiered storage | Open | Task | Minor - P4 | Disag_Storage, lc_bulk_04_29_26, tiered-storage |
| **WT-14939** | test_tiered18 not working under disagg | Backlog | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14949** | Add a check that all transactions/cursors should be closed when step down/step up happens in connection->reconfigure | Open | Task | Major - P3 | Disag_Storage |
| **WT-14950** | Update PALI doc post-discard verify routine implementation | Open | Documentation | Major - P3 |  |
| **WT-14964** | Segfault in snappy_compress via __wt_blkcache_write (test/format, tcmalloc) | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-14965** | Check possibility of an early exit in __wti_disagg_conn_config | Backlog | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14993** | Tidy up known-bad disagg tests | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-14998** | Re-enable layered tables on truncate tests | Open | Bug | Major - P3 |  |
| **WT-15010** | Rename misleading .wt_stable suffix on non-layered shared tables | Backlog | Bug | Major - P3 | Disag_Storage |
| **WT-15025** | Measure code coverage by test/model related to recovery | Open | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-15026** | Investigate and implement optimizations to re-use old disk images to avoid building full page for page deltas | Open | Improvement | Major - P3 |  |
| **WT-15027** | Add heuristic to consider building a delta if a percentage of a page's rows have been modified | Open | Improvement | Major - P3 |  |
| **WT-15032** | Better compression heuristic in block cache | Open | Improvement | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15040** | Enable testing prepared transactions in test/model | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15055** | failed: unit-test-hook-disagg-leader-table on ubuntu2004-nonstandalone [wiredtiger @ 34d32553] | Backlog | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-15056** | Fix incorrect caching mechanism for special cursors in disagg | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15057** | Ensure that the turtle file is updated atomically with the metadata file during a checkpoint | Backlog | Task | Major - P3 |  |
| **WT-15058** | Fix session->ncursors behaviour within clayered cursors | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15064** | Add table corruptions detection test cases for DisAgg tables verification | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15067** | Understand difference in tree depth between classic and disagg | Open | Task | Major - P3 | disagg-performance-investigation, lc_bulk_04_29_26, performance |
| **WT-15081** | Support prepared fast-truncate operations in disagg | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15092** | Deprecate checkpoint IDs from PALI | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15109** | Assert that pages are written in the same checkpoint in which they are reconciled | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15110** | Assertion failed: "The page checkpoint id doesn't match the current checkpoint id" | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15111** | Add disagg/layered support to data handle benchmark | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15159** | Confirm delta reconciliation behavior on history store | Open | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-15163** | Revisit how WT writes local files in Disagg mode | Open | Improvement | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15181** | Explore removing checksum from WT_PAGE_BLOCK_META | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15189** | test/format disagg follower and Python tests time out in clayered_next_random | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-15190** | Investigate whether we can use uint8_t for delta count in PALI API | Open | Technical Debt | Minor - P4 | Disag_Storage |
| **WT-15194** | Use the same macro to unpack full page images and page deltas | Open | Task | Major - P3 |  |
| **WT-15195** | Returning EBUSY from dropping layered table causes consistent state | Backlog | Task | Major - P3 |  |
| **WT-15224** | Remove WT_SESSION_QUIET_OPEN_FILE flag | Open | Technical Debt | Major - P3 | lc_bulk_04_29_26, quickwin |
| **WT-15227** | Disagg Python: Enable precise checkpoints in disagg hook for Python tests | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-15242** | Replace "wt_stable" filename check in __btree_conf | Backlog | Technical Debt | Major - P3 | lc_bulk_04_29_26 |
| **WT-15261** | Disagg testing: add switching mode to test/checkpoint | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15266** | Dump all pages from the pali response in the results array on checksum failure | Open | Task | Major - P3 |  |
| **WT-15294** | disagg: test_prepare20.py crash in checkpoint | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15313** | Add disagg functionality to wtperf so that we can re-use existing oplog testing (Nice to have) | Open | Task | Minor - P4 | lc_bulk_04_29_26 |
| **WT-15356** | Encode the checkpoint type into metadata | Open | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-15357** | disagg: support layered checkpoint cursors for last checkpoint only | Open | New Feature | Major - P3 | lc_bulk_04_29_26 |
| **WT-15364** | Add model-unit-test and model-test-failure-workloads to pull request testing | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15369** | disagg: Fix layered python tests cursor13, cursor21 that fail stats check | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15371** | disagg: fix test_hs01.py | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15372** | disagg: Fix test_verbose01.py | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15397** | Temporarily disable truncate if precise checkpoint and preserve prepared are enabled | Open | Bug | Major - P3 |  |
| **WT-15404** | Audit Python tests with logged table to maintain test coverage in disagg | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15411** | Investigate whether positioned variable is correct in clayered->remove() | Open | Bug | Major - P3 |  |
| **WT-15416** | Fix metadata cursors | Open | Task | Major - P3 | expedite, lc_bulk_04_29_26 |
| **WT-15417** | Disagg python testing: fix dropUntilSuccess errors | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15419** | Log error messages when PALI API call fail | Open | Improvement | Major - P3 | neweng |
| **WT-15434** | 5.55% decrease in Insert count in Variant ubuntu2004-perf-tests for Task perf-test-update-btree in Test update-btree.wtperf | Open | Build Failure | Major - P3 | lc_bulk_04_29_26, perf-change-point |
| **WT-15446** | Do not print large oplog in test_layered23 | Backlog | Improvement | Major - P3 |  |
| **WT-15447** | Deprecate connection config parameter disaggregated.last_materialized_lsn | Backlog | Task | Major - P3 | Disag_Storage |
| **WT-15453** | Create a dedicated API for adopting new checkpoints on the standby for disagg | Backlog | New Feature | Major - P3 | lc_bulk_04_29_26 |
| **WT-15459** | Design review and future work for metadata | Open | Task | Major - P3 | Disag_Storage, Storage_engines_outcome |
| **WT-15475** | (disagg.mode=leader) test/format failure: Truncate Invalid argument | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15476** | Validate layered table content during garbage collection | Open | Epic | Major - P3 | ds_durability_high_risk, ds_durability_mitigation |
| **WT-15521** | Clear expected errors from the error log | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15523** | Consider inlining parts of the error log | Backlog | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-15530** | Fix WT_MODIFY memory buffer error in test/format | Open | Bug | Major - P3 | Disag_Storage |
| **WT-15533** | Create a gdb macro for dumping the error log | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15540** | Remove hard limit on the number of deltas WT_DELTA_LIMIT | Open | Technical Debt | Major - P3 | lc_bulk_04_29_26 |
| **WT-15545** | Fix layered cursors ingest/stable specific statistic counting | Backlog | Task | Minor - P4 |  |
| **WT-15552** | Make precise_checkpoint configurable for disagg instead of hardcoded | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15564** | Check block is safe to cast as disagg block | Open | Technical Debt | Major - P3 | lc_bulk_04_29_26 |
| **WT-15565** | Write prepared fast truncate operation to disk | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15578** | failed: unit-test-zstd on amazon2023-armv9 [wiredtiger @ d8cd17c2] | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-15582** | Investigate large page size seen at eviction for disagg | Open | Bug | Major - P3 | disagg-performance, lc_bulk_04_29_26 |
| **WT-15585** | Update checkpoint doc for disagg | Open | Documentation | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15591** | Review the code to ensure we also check the disagg shared metadata along with the local metadata | Backlog | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15594** | Bulk cursors use unbounded memory outside of the cache | Open | Bug | Major - P3 | lc_bulk_04_29_26, or-workload-management |
| **WT-15612** | Merge last few straggling tests to develop | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15645** | Error log improvement follow-on work | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15663** | When returning a cached page from block cache, make sure to make a copy of data. | Backlog | Bug | Minor - P4 | lc_bulk_04_29_26 |
| **WT-15672** | Run WT ASAN tests with debug mode options to detect use after free of cursor data | Backlog | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-15674** | Investigate if disagg feature gaps return errors | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15679** | Memory leak in __conn_load_extension_int | Open | Bug | Minor - P4 | lc_bulk_04_29_26 |
| **WT-15684** | Make PALI implementation configurable in test/model | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15709** | Support generating page deltas for page splits | Backlog | New Feature | Major - P3 |  |
| **WT-15751** | Write ASC and DSC related stats respectively only in ASC and DSC | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15763** | Investigation: Add graceful step-down support in WT (disagg) | Backlog | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15768** | Update/remove infinite retry loop in disagg block. manager | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15770** | Fix non-disagg python testing TSAN warnings that were suppressed | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15771** | Fix lack of syncronisation between allocation and memory accesses | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15772** | Fix TAILQ lock-free synchronisation | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15783** | Investigate if we can ensure we never fail after writing the disk image in reconciliation | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15788** | test/format (disagg.mode=multi) Add leader to send ckpt metadata to follower so ckpt can be picked up. | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-15790** | Tag long running layered tests with @wttest.longtest | Backlog | Bug | Major - P3 |  |
| **WT-15808** | Support readers when performing step-up | Open | Task | Major - P3 |  |
| **WT-15818** | Consider crashing upon reading ahead of materialization frontier. | Open | Task | Minor - P4 | Disag_Storage |
| **WT-15843** | Change default cache update trigger threshold to the same as dirty trigger threshold | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15860** | Investigate how to manage internal threads during step up/down | Backlog | Task | Major - P3 |  |
| **WT-15861** | Investigate whether we should use the same root page id in disagg | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-15932** | Remove retry limit when reading metadata in disagg | Open | Task | Major - P3 |  |
| **WT-15940** | wt util fails with palite error when using a disagg config with non-disagg db | Backlog | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-15950** | Enable MSan builds for WiredTiger tests | Backlog | New Feature | Major - P3 | lc_bulk_04_29_26 |
| **WT-15958** | Refactor disagg startup to skip recovery | Open | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-15970** | During step up, fix layered cursors to wait for ingest table to drain | Open | Bug | Major - P3 |  |
| **WT-15974** | Revisit the API usage for reconfigure in disagg | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16000** | Make the checksum parameter in "checkpoint_meta" required | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16002** | Potentially unreachable pages by Checkpoint Cleanup | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-16044** | Disagg WT eviction writes duplicate phylog entries for the same page when cache is stuck | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-16072** | Failed assert layered63 test: Expected to read at least one internal delta | Backlog | Build Failure | Major - P3 | BB-Tools, Disagg |
| **WT-16084** | Design how re-reconciling in memory pages could use less I/O | Open | Task | Major - P3 | disagg-performance, lc_bulk_04_29_26 |
| **WT-16113** | Consolidate disagg leader data validation into main format stress tests | Open | Improvement | Minor - P4 | lc_bulk_04_29_26 |
| **WT-16118** | [DS] Readback and validate WT pages | Open | Task | Major - P3 | ds_durability_medium_risk, ds_durability_mitigation, lc_bulk_04_29_26 |
| **WT-16127** | test/format copy_on_file fails with ENOENT | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16129** | 28.85% increase in cursor_modify_instructions in Variant amazon2023-perf-tests-arm64-only for Task cppsuite-api-instruction-count-benchmarks-default-perf in Test api_instruction_count_benchmarks | Backlog | Build Failure | Major - P3 | lc_bulk_04_29_26, perf-change-point |
| **WT-16134** | Enable test/format to run using PALI instead of PALite. | Open | Task | Major - P3 |  |
| **WT-16136** | Version cursor: determine if the stop durable timestamp is from a tombstone or the previous full value for HS | Backlog | Technical Debt | Major - P3 |  |
| **WT-16148** | Investigate why version cursor cannot access the HS entry | Open | Task | Major - P3 |  |
| **WT-16149** | Align the disagg sanitizers test coverage with the classic WT | Backlog | Build Failure | Major - P3 | lc_bulk_04_29_26 |
| **WT-16155** | Add optional reopen support to format_test_script | Open | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-16159** | Enable multi-process DB access in PALite | Backlog | New Feature | Major - P3 |  |
| **WT-16188** | Ensure that checkpoint pick up scales to millions of tables | Open | Task | Major - P3 |  |
| **WT-16197** | Add more Python disagg tests to code coverage tracking | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16198** | Generate disagg-only coverage report | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16224** | Unpack the internal page deltas and base page progressively during the merging process | Open | Task | Major - P3 |  |
| **WT-16225** | Create guidelines for removing/deprecating config | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16226** | Use the --skip-tests-in-file Python runner flag to replace Evergreen unit_test_ignore variable | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16228** | Revisit increased cache size for disagg (Post WT-16134) | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16232** | Complete connection sweeps should run for disagg | Backlog | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-16238** | failed: format-stress-test-disagg-follower-2 on ubuntu2004-arm64-nonstandalone [wiredtiger @ 3b91a761] | Backlog | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16239** | Write a full page instead of delta if we have a lot of deletes on the page become globally visible | Open | Improvement | Major - P3 | perf-improvement |
| **WT-16255** | Improve efficiency of rewriting large in-memory pages | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16256** | Create a python unit test framework for wt_binary_decode | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16257** | Add oldest timestamp to checkpoint metadata | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16258** | Scan and flag active transactions for rollback when picking up a checkpoint | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16259** | Understand prepared transactions behaviour when failing a transaction due to expired history | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16260** | Expired history testing | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16276** | test_cursor18 prepared value assertion error | Backlog | Build Failure | Major - P3 | BB-Tools, Disagg |
| **WT-16277** | test_cursor13 cursor cache failure | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16389** | `windows_shim.h` header breaks C++ standard streams headers | Backlog | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-16399** | Set image_size disagg metadata for checkpoint root and checkpoint metadata pages. | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16439** | failed: unit-test-hook-disagg-leader-extra-long on rhel80 [wiredtiger @ c1037599] | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16442** | Write Performance Reconciliation Efficiency - Delta Generation for re-split pages | Open | Task | Major - P3 |  |
| **WT-16452** | Invalid dhandle access in setRecoveryCheckpointMetadata | Open | Bug | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-16454** | Investigate why mongod is stalling with 1-8GB as cache size | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16462** | Improve how __create_table determines whether a table is layered | Open | Technical Debt | Major - P3 |  |
| **WT-16470** | cursor hangs inside search() | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16474** | test_layered71 drop returns EBUSY | Open | Build Failure | Major - P3 | BB-Tools |
| **WT-16477** | Read shared metadata directly when opening the dhandle on a shared table on standby to avoid taking the checkpoint lock | Open | Improvement | Major - P3 |  |
| **WT-16478** | Create a verify section in the architecture guide | Open | Documentation | Major - P3 | lc_bulk_04_29_26 |
| **WT-16481** | Allow test/format multi-node mode to work with database reopen | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-16494** | Ensure checkpoint order is strictly increasing across nodes in disagg | Open | Task | Major - P3 | Disag_Storage, dc, durability-SDAP, lc_bulk_04_29_26 |
| **WT-16511** | Consider adding original_checksum field to disagg block header | Open | Task | Major - P3 | lc_bulk_04_29_26, wt_data_format |
| **WT-16525** | Remove WT_PAGE_LOG_LSN_MAX | Open | Technical Debt | Major - P3 |  |
| **WT-16529** | Increase eviction queue usage when the queue is frequently empty | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16532** | Investigate failure from bug010 | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-16535** | Ensure WT_PAGE_LOG_ENCRYPTED is default set for regular tables | Open | Task | Major - P3 |  |
| **WT-16541** | Failed: unit-test-macos on macOS 14 (ARM64) [WiredTiger (develop) @ 1fc7664f] | Open | Build Failure | Major - P3 | lc_bulk_04_29_26 |
| **WT-16544** | Investigate slow checkpoint pick-up behaviour | Open | Task | Major - P3 |  |
| **WT-16549** | failed: test_encrypt01 on macos-14-arm64 [wiredtiger @ 47d98033] | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16553** | failed: unit-test-macos on macos-14-arm64 [wiredtiger @ 3259ea43] | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16562** | Checkpoint size tech debt cleanup | Open | Technical Debt | Major - P3 | lc_bulk_04_29_26 |
| **WT-16586** | failed: unit-test-hook-disagg-leader-bucket00 on ubuntu2004-asan [wiredtiger @ ef85bc68] | Backlog | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16624** | s-outdated-fixmes does not account for feature branches, failing infrequent-checks [wiredtiger @ 57b822fe] | Open | Technical Debt | Major - P3 | lc_bulk_04_29_26 |
| **WT-16627** | Coverity analysis defect 202218: Check of thread-shared field evades lock acquisition | Open | Bug | Major - P3 | coverity, expedite, lc_bulk_04_29_26, wiredtiger.develop |
| **WT-16629** | Investigate ways to bring back performance that was lost due to a correctness fix | Backlog | Build Failure | Minor - P4 | lc_bulk_04_29_26, perf-change-point |
| **WT-16649** | Convert disagg.mode to enum | Open | Task | Minor - P4 | lc_bulk_04_29_26, neweng |
| **WT-16660** | bytes_total increment not protected by reconciliation panic boundary | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-16663** | Out-of-order timestamp assertion failure in WiredTiger during patch build | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-16668** | Determine cause of  Palite indirect leak LSan failure | Backlog | Improvement | Major - P3 |  |
| **WT-16692** | failed: unit-test-hook-disagg-leader-tsan-bucket00 on amazon2023-arm64-tsan [wiredtiger @ 02fa66fe] | Backlog | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16720** | Validation improvements in disagg | Open | Epic | Major - P3 | lc_bulk_04_29_26 |
| **WT-16732** | Spike: Extending predictable replay to support truncate operations | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16734** | Enable disagg testing (-G) for schema abort tests as part of crash recovery scenario testing | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16736** | Enable test/format (disagg) multi node in evergreen variants | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16737** | Enable timestamp_abort in relevant evergreen variants | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16748** | Support decoding cells with timestamps as bson | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16763** | TSAN data race warning in __wti_txn_get_pinned_timestamp vs __wt_txn_global_set_timestamp | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16773** | Refactor conn_layered.c into seperate files pt.3 | Open | Task | Major - P3 |  |
| **WT-16775** | Investigate if we still need to disable disagg config in test model workload generator | Open | Task | Minor - P4 | lc_bulk_04_29_26, neweng |
| **WT-16783** | Data race __statlog_config & __wt_cond_wait_signal | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16790** | WiredTiger Eviction Stalling In Disagg | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16806** | Enable Windows build for PALite | Open | Task | Major - P3 |  |
| **WT-16810** | Clarify layered cursor invariants under disagg leader promotion | Backlog | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-16813** | (Follower mode) Implement garbage collection checkpoint pick-up with fast truncate design | Open | Task | Major - P3 |  |
| **WT-16824** | Refactor verify string helpers to return error codes | Open | Technical Debt | Minor - P4 | lc_bulk_04_29_26 |
| **WT-16825** | __wt_btree_open, 213: WiredTiger assertion failed on lock_flags while getting checkpoint lock | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16833** | Add Dynamic Checkpoint Eviction Triggers to Eviction API | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16837** | Investigate whether the stat log server should process ingest tables on leader | Open | Task | Major - P3 |  |
| **WT-16851** | Eliminate the need to create missing ingest btrees when loading a new checkpoint | Open | Task | Major - P3 |  |
| **WT-16853** | Cleanup some layered table stats | Backlog | Technical Debt | Major - P3 | lc_bulk_04_29_26 |
| **WT-16855** | task-timed-out: unit-test-hook-disagg-leader-tsan-bucket03 on amazon2023-arm64-tsan [wiredtiger @ 74faf919] | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16856** | task-timed-out: unit-test-hook-disagg-leader-tsan on amazon2023-arm64-tsan [wiredtiger @ 74faf919] | Backlog | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16864** | failed: model-test-long-random-config-disagg on ubuntu2004-asan [wiredtiger @ fb837464] | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16870** | Review the workflow of reopen_disagg_conn to disable shutdown checkpoint | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16873** | disagg: Fix unintentional skipping of non-tiered tests | Backlog | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-16877** | Make __wt_layered_table_manager.leader to be wt_shared | Open | Task | Major - P3 |  |
| **WT-16879** | Fix the potential data race between open btree or open dhandle and primary step down | Open | Task | Major - P3 | Disag_Storage, lc_bulk_04_29_26 |
| **WT-16885** | Unexpected requirement for WiredTiger changes to enable prefetch during disagg perf testing | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16899** | failed: unit-test-hook-disagg-leader-macos test_hs21 unexpected base write gen | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-16901** | Introduce a failpoint prior to checkpoint completion | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16918** | Implement tableExists() for disagg python tests | Open | Task | Major - P3 | disaggregated-storage, lc_bulk_04_29_26 |
| **WT-16919** | Improve error reporting on checksum mismatch | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16920** | Support per home directory uri tracking for disagg | Backlog | Task | Minor - P4 | disaggregated-storage, lc_bulk_04_29_26 |
| **WT-16931** | Add metadata helpers to Python test framework | Backlog | Improvement | Minor - P4 | lc_bulk_04_29_26 |
| **WT-16960** | Better create/drop table inclusion logic for disagg storage checkpoints | Open | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-16961** | Implement temporary "best effort" truncate for disagg | Open | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-16978** | Enable WT_CURSOR::modify() in MongoDB for DisAgg | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-16982** | Provide long term solution layered dhandles and ingest tables | Backlog | Task | Major - P3 | Disag_Storage |
| **WT-17008** | 3.92% increase in disagg_step_up_time in Variant amazon2023-perf-tests-arm64-only for Task cppsuite-disagg-failover-perf-append in Test test_disagg_failover_perf | Backlog | Build Failure | Major - P3 | lc_bulk_04_29_26, perf-change-point |
| **WT-17013** | In checkpoint_tree fix layering violation when freeing disagg ckpt root pages | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17023** | Disagg test fails with "reconciliation failed after building the disk image" | Open | Build Failure | Major - P3 | lc_bulk_04_29_26 |
| **WT-17034** | Fix bytes_total not being rolled back on addr_pack failure in reconciliation | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-17040** | Investigate whether the creation of shared metadata is necessary on followers | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17049** | Avoid reopening the stable table for each operation on leader | Backlog | Improvement | Major - P3 | perf-improvement |
| **WT-17050** | Avoid doing the search to check existing keys for layered insert and update on leader | Backlog | Improvement | Major - P3 | perf-improvement |
| **WT-17061** | Set close idle time to sweep server for disagg follower node | Open | New Feature | Major - P3 | lc_bulk_04_29_26 |
| **WT-17062** | Tune shared_checkpoint_handle_close_idle_time | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17063** | Support shared disk hash table switch in node step down and step up | Open | New Feature | Major - P3 | lc_bulk_04_29_26 |
| **WT-17066** | Investigate and define shared disk hash table bucket size | Open | Task | Major - P3 |  |
| **WT-17087** | Add publish API and implement it for the leaders | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17088** | Assert that we do not write data to an unpublished table | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17089** | Implement the publish functionality for the followers | Open | Task | Major - P3 |  |
| **WT-17090** | Reconcile checkpoint pick-up with metadata operations on the follower | Open | Task | Major - P3 |  |
| **WT-17091** | Investigate and implement step-down for publish | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17093** | Redefine the rules of checkpoint order for fake checkpoint | Open | Task | Major - P3 |  |
| **WT-17099** | test_layered71.py unit-test on amazon2023-arm64 [wiredtiger @ fc53678f] | Open | Build Failure | Major - P3 | BB-Tools |
| **WT-17105** | Disagg Bugs | Open | Epic | Major - P3 | lc_bulk_04_29_26 |
| **WT-17125** | Allow verify to continue past read errors in disaggregated storage | Backlog | Task | Major - P3 |  |
| **WT-17127** | Update metadata verification check in bt_vrfy.c to use WT_IS_URI_METADATA | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17131** | Follower layered cursors should not reopen an unchanged stable table at checkpoint pick up | Backlog | Improvement | Major - P3 |  |
| **WT-17135** | (Follower mode) Enable fast truncate on develop | Open | Task | Major - P3 |  |
| **WT-17138** | Add testing with debug_mode=(cursor_copy=true) under ASAN | Open | Task | Major - P3 | expedite, lc_bulk_04_29_26 |
| **WT-17141** | Unreachable code in __clayered_reserve | Open | Technical Debt | Major - P3 | expedite |
| **WT-17146** | Add a shared metadata consistency check to the verify | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17160** | Increasing the number of situations in test_layered91.py results in abort due to cache stuck. | Backlog | Bug | Major - P3 | disaggregated-storage |
| **WT-17173** | Throughput reduction caused by futile eviction walks on ingest trees in disaggregated storage | Open | Improvement | Major - P3 | lc_bulk_04_29_26 |
| **WT-17174** | Fix use of "readonly" configuration by layered cursors, and make readonly cursors cacheable | Open | Bug | Major - P3 |  |
| **WT-17177** | Investigate whether support for read-only connections in disagg is needed | Backlog | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17188** | Extend btree ID uniqueness verification to shared (disagg) metadata | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17189** | During GC, verify the most recent update against the stable table in debug build | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17192** | During GC, verify the most recent update against the stable table in release build | Open | Task | Major - P3 |  |
| **WT-17205** | test_layered38 test_gc_ingest_with_no_open_cursor assertion error | Backlog | Build Failure | Major - P3 | BB-Tools |
| **WT-17224** | [Disagg Testing Gaps] Revisit all the test cases disabled for Disagg | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17225** | [Disagg Testing Gaps] Analyse open Jira tickets and FIXMEs to identify testing gaps | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17226** | [Disagg Testing Gaps] Compile all the reports from previous steps to the final one | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17247** | Layered cursor writes on follower do not check stable cell's full time window | Open | Bug | Critical - P2 | dc, disagg, expedite, layered-cursor, lc_bulk_04_29_26 +1 |
| **WT-17250** | Add validation test for shared disk cache | Open | Task | Major - P3 |  |
| **WT-17253** | ThreadSanitizer: Data race in SpillWiredTigerKVEngine::cleanShutdown - Shutdown Session/Sweep Race | Open | Bug | Major - P3 | expedite, lc_bulk_04_29_26 |
| **WT-17263** | Refactor __clayered_search_near_int into focused helpers | Backlog | Task | Major - P3 |  |
| **WT-17278** | Follower remove returns WT_NOTFOUND where leader returns WT_ROLLBACK, causing data mismatch in multi-node validation | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-17296** | Merge cross checkpoint caching feature branch | Open | Task | Major - P3 |  |
| **WT-17300** | __curstat_size_only fast path incorrectly propagates ENOENT instead of falling back to slow path | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-17301** | Investigate why we are having .wt files in a disagg mdb run | Open | Task | Major - P3 | expedite, lc_bulk_04_29_26 |
| **WT-17307** | Creating large numbers of tables causes standby lag | Open | Task | Major - P3 |  |
| **WT-17309** | Support step-up without resetting all the cursors | Backlog | Task | Major - P3 |  |
| **WT-17312** | Investigate RandomCursor hang on Standby from analyzeShardKey command | Open | Bug | Major - P3 | lc_bulk_04_29_26 |
| **WT-17316** | failed: format-stress-test-disagg-leader-data-validation-1 on ubuntu2004-stress-nonstandalone [wiredtiger @ 9d92d192] | Open | Build Failure | Major - P3 | BB-Tools, lc_bulk_04_29_26 |
| **WT-17319** | Provide more information when failing to pickup a checkpoint in disagg | Open | Task | Major - P3 | lc_bulk_04_29_26, quickwin |
| **WT-17323** | Layered table cursors are not swept, leading to potential file descriptor exhaustion | Open | Task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17327** | Document the stable schema epoch | Open | Task | Major - P3 |  |
| **WT-17330** | Spike: evaluate the performance of the layered-table truncate list | Open | Improvement | Major - P3 |  |
| **WT-17338** | Auto-pick up latest checkpoint in disagg follower mode for wt tool | Open | Sub-task | Major - P3 | lc_bulk_04_29_26 |
| **WT-17340** | test/format (disagg.mode=switch) out-of-order timestamp update detected during reconciliation | Open | Build Failure | Major - P3 | lc_bulk_04_29_26 |
| **WT-17341** | Add wt util subcommand to read a single page through WT_PAGE_LOG | Open | Sub-task | Major - P3 |  |
| **WT-17342** | Check acquire/release semantics in __wt_txn_global_set_timestamp | Open | Technical Debt | Major - P3 | lc_bulk_04_29_26 |
| **WT-17343** | Refactor layered cursor next_random to remove file-cursor layering violation | Backlog | Task | Major - P3 |  |
| **WT-17344** | Add wt util subcommand to dump the turtle page | Open | Sub-task | Major - P3 |  |
| **WT-17345** | Reject wt util args that should not be supported in disaggregated storage mode | Open | Sub-task | Major - P3 |  |
| **WT-17346** | Reject wt subcommands that are unsupported in disagg mode. | Open | Sub-task | Major - P3 |  |
| **WT-17349** | Support reading individual pages in follower mode without checkpoint pickup | Open | Sub-task | Major - P3 |  |
| **WT-17351** | Document the wt util in disagg mode | Open | Sub-task | Major - P3 |  |
| **WT-17352** | WT (Disaggregated Storage ) Checkpoint Pickup Performance | Open | Epic | Major - P3 |  |
| **WT-17367** | Mirror mismatch error in format-stress-test-disagg-switch-data-validation-3 on amazon2023-disagg-stress [wiredtiger-mongo-v8.3 @ 49cac3c5] | Open | Build Failure | Major - P3 | BB-Tools, expedite |
| **WT-17380** | Enable prepare for test/format disagg switch mode on mainline | Open | Task | Major - P3 |  |

---

## All Other Tickets (598)

| Key | Summary | Status | Type | Priority | Labels |
|-----|---------|--------|------|----------|--------|
| WT-13956 | cache stuck in linux-directio on 8.0 | Backlog | Build Failure | Major - P3 | BB-Tools, cache-stuck |
| WT-13957 | Improve the arch-toc-int-wt-dev.html page | Backlog | Documentation | Major - P3 | neweng |
| WT-13969 | Coverity analysis defect 163865: Missing unlock | Backlog | Bug | Minor - P4 | wt-atomic |
| WT-13974 | Make argument order and session type consistent in live restore code. | Backlog | Technical Debt | Minor - P4 | Modularity, code-quality |
| WT-13976 | Document how WiredTiger manages concurrent access to BTree data structures | Backlog | Improvement | Major - P3 | doc, documentation |
| WT-13983 | Show python stack when WT aborts | Backlog | Improvement | Major - P3 | diagnostics |
| WT-13984 | Update WiredTigers documentation with respect to phantom reads under snapshot isolation | Backlog | Documentation | Minor - P4 |  |
| WT-13986 | dhandle not closed after drop in test_sweep03.py | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-13987 | cache stuck in test_txn27.test_rollback_reason | Backlog | Build Failure | Major - P3 | BB-Tools, cache-stuck |
| WT-13992 | task-timed-out: spinlock-pthread-adaptive-test on amazon2-arm64 [wiredtiger @ 9f98ce96] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-13996 | test/format FORMAT_FAILED_TO_KILL_PARENT_THREAD | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-13997 | 4.17% increase in commit_transaction_instructions in ubuntu2004-perf-tests-arm64-only/ cppsuite-api-instruction-count-benchmarks-default-perf/ api_instruction_count_benchmarks - 0dc7ed4, 2025-01-07 | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-13998 | Replace some named constants with context-aware ones. | Backlog | Improvement | Major - P3 | code-quality |
| WT-14000 | 5.06% decrease in Insert count in ubuntu2004-perf-tests/ perf-test-update-btree/ update-btree.wtperf - 84a9c4e, 2025-01-09 | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-14010 | Create Network Graphs to showcase module coupling | Backlog | Task | Major - P3 | Modularity |
| WT-14011 | Enable the access_check script to be executed in PR testing | Backlog | Task | Major - P3 |  |
| WT-14018 | start_lsn isn't zero initialised in 464wt_print_debug_log | Backlog | Bug | Minor - P4 | neweng |
| WT-14021 | Potential write to null pointer in __wt_struct_packv | Open | Bug | Major - P3 | code-quality |
| WT-14028 | Investigate if maybe-uninitialized command line option in gcc does what we need | Backlog | Improvement | Major - P3 | code-quality, compilation |
| WT-14029 | Add timing stress config to live restore | Open | Improvement | Major - P3 | backport-confidence, code-quality |
| WT-14031 | The op_timer_fired mechanism doesn't free application threads stuck in eviction after commit or rollback | Backlog | Bug | Major - P3 | code-quality |
| WT-14032 | Hang analyzer fails to analyze timed out c tests. | Backlog | Bug | Minor - P4 | quick-win |
| WT-14037 | Eviction gets stuck due to server enqueuing non-evictable pages. | Backlog | Bug | Major - P3 | code-quality, investigation |
| WT-14038 | Document the benefits of using a smaller leaf_page_max setting for accessing random documents in out of cache datasets. | Open | Task | Major - P3 | documentation |
| WT-14043 | Improve documentation of the "busy" function argument in eviction | Backlog | Improvement | Major - P3 | quick-win |
| WT-14047 | Support absolute paths to the WiredTiger log directory | Backlog | Improvement | Major - P3 | code-quality |
| WT-14051 | Support Windows for live restore | Backlog | Improvement | Minor - P4 | code-quality |
| WT-14055 | Investigate consolidation of literal error messages | Backlog | Task | Major - P3 | sprint_add_project |
| WT-14059 | Coverity analysis defect 168237: Dereference before null check | Backlog | Bug | Minor - P4 | code-quality |
| WT-14064 | Refine condition to exit "__wti_evict_app_assist_worker" with "busy" flag set | Backlog | Improvement | Major - P3 | code-quality, perf |
| WT-14071 | Infinite loop when trying to commit data larger than the cache | Open | Bug | Minor - P4 | code-quality |
| WT-14081 | Cut WiredTiger 12.0.0 release | Backlog | Task | Major - P3 | open-source-release |
| WT-14082 | Investigate whether internal threads should be calling __wti_evict_app_assist_worker | Backlog | Task | Major - P3 | code-quality |
| WT-14083 | Ambiguity of ignore return commit/rollback transaction and eviction | Backlog | Task | Major - P3 | diagnostics |
| WT-14084 | Document perf_run_py | Backlog | Documentation | Major - P3 | doc, quick-win |
| WT-14089 | Perf improvement for next/prev in the presence of cursor bounds | Open | Improvement | Major - P3 | performance |
| WT-14092 | format-stress-test-2 ran out of disk space | Backlog | Build Failure | Minor - P4 | BB-Tools |
| WT-14094 | Refactor evergreen perf tasks | Backlog | Task | Trivial - P5 | neweng |
| WT-14098 | Re-enable python test checkpoint33 in TSan testing | Backlog | Task | Minor - P4 | code-quality |
| WT-14101 | Reduce PR test time for python test suite | Backlog | Improvement | Major - P3 | SEKB, test |
| WT-14103 | Calling dump() crashes when we call drop() | Backlog | Bug | Major - P3 | diagnostics |
| WT-14112 | Add more statistics to track why the pages with updates being skipped as part of the eviction walk | Backlog | Task | Major - P3 | diagnostics |
| WT-14116 | Investigate whether windows locking mechanism is strict enough | Backlog | Task | Major - P3 | wt-atomic |
| WT-14124 | Remove ARMv8 build variants from evergreen after the rollout of ARMv9 in Atlas | Open | Improvement | Major - P3 | test |
| WT-14126 | Delete build variants or tests that no longer need to run on x86 | Open | Improvement | Major - P3 | test |
| WT-14128 | Add default statement for our switch cases | Backlog | Task | Major - P3 |  |
| WT-14138 | Remove open file type WT_FS_OPEN_FILE_TYPE_CHECKPOINT | Backlog | Technical Debt | Minor - P4 | code-quality |
| WT-14141 | Segmentation fault when accessing dsk->write_gen != 0 | Backlog | Build Failure | Major - P3 | BB-Tools, code-quality |
| WT-14143 | Prepare WiredTiger release notes for version 8.1 | Open | Task | Major - P3 | open-source-release |
| WT-14144 | Should application threads avoid update eviction when "busy" | Backlog | Improvement | Minor - P4 | code-quality |
| WT-14153 | Investigate the impact of aggressive obsolete cleanup | Backlog | Task | Major - P3 | investigation |
| WT-14159 | Refactor the util_dump() function to reduce its complexity | Backlog | Improvement | Major - P3 | Modularity |
| WT-14161 | bulk-load is only supported on newly created objects in unit-test-hook-tiered-with-delays | Backlog | Build Failure | Minor - P4 | BB-Tools |
| WT-14164 | catch2 sub_level_error_drop_uncommitted_dirty does not return EBUSY when expected | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14165 | test_tiered14 failed with ListObjects request to S3 failed | Backlog | Build Failure | Minor - P4 | BB-Tools |
| WT-14167 | compact interrupted by application in csuite-wt8057-compact-stress-test on 7.0 | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14179 | Change compile mongodb to align with how mongo compiles sys-perf | Backlog | Improvement | Major - P3 |  |
| WT-14189 | Create evergreen tags to categorize core tests | Open | Task | Major - P3 | dev-prod |
| WT-14190 | Investigate whether it is possible to combine all sanitizer variants into one | Backlog | Task | Major - P3 | dev-prod |
| WT-14196 | Investigate compact performance under concurrent write workloads | Backlog | Task | Major - P3 | diagnostics |
| WT-14203 | Convert checkpoint related EBUSY messages into sub-level error code | Backlog | Task | Major - P3 | code-quality, diagnostics, neweng |
| WT-14214 | ASan: Out of bounds access in __wt_cell_type_raw | Backlog | Build Failure | Major - P3 | BB-Tools, dc, na-mdb |
| WT-14223 | Support read only database connections with live restore | Backlog | New Feature | Minor - P4 | code-quality |
| WT-14224 | Check uses of non-atomic "conn" and "dsrc" stat macros. | Backlog | Bug | Major - P3 |  |
| WT-14225 | Create a test_live_restore base class for python tests | Backlog | Technical Debt | Major - P3 | code-quality |
| WT-14227 | Add a hook in SWIG to print sub_level_error values when an error is return | Open | Technical Debt | Major - P3 | diagnostics |
| WT-14236 | Save more information when the connection cannot be closed gracefully | Backlog | Task | Major - P3 |  |
| WT-14245 | Eviction FTDC not reflecting eviction stat correctly | Backlog | Improvement | Major - P3 | diagnostics |
| WT-14248 | Tests should generate tables in a home directory below the test | Backlog | Task | Major - P3 | code-quality |
| WT-14253 | Improve formatting for WiredTiger startup/shutdown performance logs | Backlog | Improvement | Major - P3 | API, dignostics |
| WT-14254 | Update HELP Playbook for HELP-71583 | Open | Task | Major - P3 | diagnostics |
| WT-14258 | Add evergreen tests for currently supported GCC and Clang version (pt.2) | Backlog | Task | Major - P3 | dev-prod |
| WT-14263 | Introduce a new job scheduling mode in test/format to simulate thundering herd effect | Backlog | Improvement | Major - P3 | code-quality, dev-prod |
| WT-14264 | Improve failure detection in test/format script | Backlog | Improvement | Major - P3 | dev-prod |
| WT-14265 | test_checkpoint failure assert detects evicting an accessible internal page with an active split generation | Open | Build Failure | Major - P3 | BB-Tools |
| WT-14266 | Prepare conflict in workgen-test-prepare_stress | Open | Build Failure | Major - P3 | BB-Tools |
| WT-14272 | Set default CC_OPTIMIZE_LEVEL to O0 for all tasks | Backlog | Task | Major - P3 | dev-prod, diagnostics |
| WT-14283 | csuite tests delete given database when verifying | Open | Improvement | Minor - P4 | quick-win |
| WT-14285 | Add tree walk statistics that allow for inferring I/O per operation | Open | Improvement | Major - P3 | diagnostics |
| WT-14286 | failed: make-check-test on ubuntu2004-msan [wiredtiger @ 675da6ce] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-14287 | failed: unit-test on rhel8-zseries [wiredtiger @ 2a18ceea] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14288 | Csuite tests fail with paths appended with / | Backlog | Bug | Major - P3 | dev-prod, investigate |
| WT-14296 | Investigate UBSan test behaviour on evergreen | Backlog | Task | Major - P3 | dev-prod |
| WT-14300 | The txn_bytes_dirty counter should not get reset on reset_snapshot() | Backlog | Bug | Minor - P4 | diagnostics |
| WT-14308 | Understand WT read behavior during in-cache 100% update YCSB workload | Open | Task | Minor - P4 |  |
| WT-14313 | Add an API call for the cache pressure formula from WT-14075 | Open | Task | Major - P3 |  |
| WT-14318 | Shrink the live restore bitmap when possible | Backlog | Improvement | Major - P3 |  |
| WT-14320 | failed: format-stress-test-no-barrier on ubuntu2004-stress-tests [wiredtiger-mongo-v7.0 @ f70f8426] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-14321 | failed: format-stress-sanitizer-test-1 on amazon2023-stress-tests-armv9 [wiredtiger @ ad3a9ff8] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-14322 | Investigate why the incremental backup needs to open all dhandles | Open | Task | Major - P3 | diagnostics |
| WT-14324 | Reduce the number of WiredTiger statistics returned | Open | Improvement | Major - P3 | diagnostics |
| WT-14329 | Segmentation fault in format-abort-recovery-stress-test | Open | Build Failure | Minor - P4 | BB-Tools, na-mdb |
| WT-14331 | Fast truncate information written to disk | Open | Build Failure | Major - P3 | BB-Tools, dc, na-mdb |
| WT-14335 | Fix test_syscall on MacOS and enable this configuration in CI | Backlog | Task | Major - P3 | code-quality |
| WT-14337 | Investigate / Characterise efficiency of App threads evicting dirty content. | Open | Task | Major - P3 |  |
| WT-14338 | Investigate if compaction should be pulled into eviction or throttled before reaching the trigger thresholds | Open | Task | Major - P3 |  |
| WT-14339 | Include information about source file name and line number in logging | Open | Task | Major - P3 | code-quality |
| WT-14346 | The executable path retrieved in print_stack_trace.py is incorrect | Backlog | Bug | Major - P3 |  |
| WT-14347 | Make `wt verify` check if the keys come in order | Backlog | Improvement | Major - P3 |  |
| WT-14351 | Reduce the chance of RNG synchronization between sessions | Backlog | Improvement | Major - P3 |  |
| WT-14358 | failed: unit-test on amazon2023-armv9-nonstandalone [wiredtiger @ 0458a5cb] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14366 | Fix test_checkpoint25 & test_checkpoint24 not able to perform fast truncate operation | Open | Build Failure | Major - P3 | BB-Tools |
| WT-14369 | Investigate converting the EINVAL to EBUSY in __session_open_cursor_int | Backlog | Technical Debt | Minor - P4 |  |
| WT-14373 | Identify and catalog deterministic/non-deterministic tests | Open | Task | Major - P3 |  |
| WT-14374 | Reorganize all deterministic & predictable tests in evergreen | Open | Task | Major - P3 |  |
| WT-14375 | Fix test_wt8246_compact_rts_data_correctness failure due to WT_ROLLBACK | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-14377 | Create a list of variants types that are used by altas | Open | Task | Major - P3 |  |
| WT-14378 | Create skeletal code generate structure | Open | Task | Major - P3 | dev-prod |
| WT-14379 | Code generate all related python tests in evergreen | Backlog | Task | Major - P3 | dev-prod |
| WT-14380 | Code generate all csuite tests | Backlog | Task | Major - P3 | dev-prod |
| WT-14381 | Code generate all performance testing | Backlog | Task | Major - P3 | dev-prod |
| WT-14382 | Provide descriptive warnings/error messages for disk related errors | Backlog | Task | Minor - P4 |  |
| WT-14386 | Check correctness and convenience of the Python infrastructure for TSan | Backlog | Task | Major - P3 |  |
| WT-14395 | Crash during a checkpoint should not advance the oldest timestamp | Open | Bug | Major - P3 | model-test |
| WT-14398 | Create OOO databases and expect WT to catch these cases | Backlog | Task | Major - P3 |  |
| WT-14400 | 17.93% decrease in Checkpoint count in Variant amazon2023-perf-tests-arm64 for Task perf-test-long-checkpoint-stress in Test checkpoint-stress.wtperf | Open | Build Failure | Major - P3 | perf-change-point |
| WT-14404 | 45.77% increase in Times insert stalled in Variant ubuntu2004-perf-tests for Task many-collection-test in Test many-collection-test | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-14405 | 19.33% decrease in Update count in Variant ubuntu2004-perf-tests-arm64 for Task perf-test-long-checkpoint-stress in Test checkpoint-stress.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-14450 | 29.78% increase in Load time in Variant ubuntu2004-perf-tests for Task perf-test-evict-btree in Test evict-btree.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-14452 | 28.47% increase in Latency(read, update) Max1 in Variant amazon2023-perf-tests-arm64 for Task perf-test-long-500m-btree-50r50u in Test 500m-btree-50r50u.wtperf | Open | Build Failure | Major - P3 | perf-change-point |
| WT-14453 | 16.46% increase in Average checkpoint duration in Variant amazon2023-perf-tests-arm64 for Task many-collection-test in Test many-collection-test | Open | Build Failure | Major - P3 | perf-change-point |
| WT-14460 | Ensure WT errors are expressed to server code in standard JSON | Open | Task | Major - P3 |  |
| WT-14462 | Add an automatic check to verify that TCMalloc is loaded during CI testing | Backlog | Task | Major - P3 |  |
| WT-14464 | Check whether Evergreen uses right TCMalloc version | Backlog | Task | Major - P3 |  |
| WT-14465 | PRs raised by external contributors fail to get the PR template comment | Backlog | Bug | Minor - P4 |  |
| WT-14466 | Improve help message for verify_only mode in abort tests | Backlog | Bug | Minor - P4 |  |
| WT-14477 | Test failure in test_compact12 when increasing the size of WT_PAGE_MODIFY | Backlog | Bug | Major - P3 |  |
| WT-14556 | Offloading compress to achieve better performance | Open | Task | Minor - P4 |  |
| WT-14558 | incr_backup test artefacts are nested but should be on the same level | Open | Bug | Minor - P4 | dev-prod |
| WT-14560 | Update README in test/csuite directory | Backlog | Documentation | Minor - P4 |  |
| WT-14564 | Investigate WiredTiger metadata corruption detected error while recovering from logs. | Open | Bug | Major - P3 |  |
| WT-14578 | Return a sub-level error code when exclusive access is prevented by a live restore background cursor | Backlog | Improvement | Major - P3 |  |
| WT-14584 | Rename functions in pow.c | Backlog | Improvement | Minor - P4 | code-quality, neweng |
| WT-14593 | Be able to configure the allocation size in test/format | Backlog | Improvement | Major - P3 |  |
| WT-14594 | Python tests can error out with a very esoteric error, improve it. | Backlog | Improvement | Major - P3 | neweng |
| WT-14595 | Incorrect calculation of max time spent building page image | Backlog | Bug | Major - P3 |  |
| WT-14599 | evict->read_gen_oldest can be far in the future | Backlog | Bug | Major - P3 |  |
| WT-14604 | Shrink codeowners to those that work in WT | Open | Task | Major - P3 |  |
| WT-14618 | Investigate catch2 test failing on Linux VMs | Open | Bug | Major - P3 |  |
| WT-14622 | test_prepare_hs03 fails with missing keys not checked | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14639 | failed: unit-test-hook-tiered on ubuntu2004 [wiredtiger @ 9354a894] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14640 | Automate new sub-level error generation | Open | Improvement | Major - P3 | neweng |
| WT-14651 | __wt_btcur_modify calls btcur_search even if the cursor is already positioned | Backlog | Task | Major - P3 |  |
| WT-14667 | Understand why connection close may return EBUSY | Backlog | Task | Major - P3 |  |
| WT-14668 | Update WiredTiger Release Notes for SPM-4278 | Open | Task | Major - P3 |  |
| WT-14671 | Add log when foreground compact finishes a file | Open | Task | Minor - P4 |  |
| WT-14675 | Document scrub eviction behaviour and stats | Backlog | Task | Major - P3 |  |
| WT-14688 | Improve live restore server test coverage | Backlog | Task | Minor - P4 |  |
| WT-14691 | Implement "config:" cursor | Backlog | New Feature | Major - P3 |  |
| WT-14694 | Add an ability to configure how many times should "run_format_configs.sh" run certain tests | Backlog | Bug | Major - P3 | neweng |
| WT-14701 | Update WiredTiger Release Notes for SPM-4282 | Open | Task | Major - P3 |  |
| WT-14702 | Update WiredTiger Release Notes for SPM-4283 | Open | Task | Major - P3 |  |
| WT-14703 | Update WiredTiger Release Notes for SPM-4285 | Open | Task | Major - P3 |  |
| WT-14712 | Update WiredTiger Release Notes for SPM-4289 | Open | Task | Major - P3 |  |
| WT-14714 | Remove flag WT_CURBACKUP_RENAME from WiredTiger and tidy up | Open | Improvement | Major - P3 |  |
| WT-14745 | Records mismatch inside backup test | Open | Bug | Major - P3 |  |
| WT-14757 | LSM thread panic in test_backup_target (7.0) | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-14768 | Log debugging information for both active transactions and cursors blocking RTS | Backlog | Improvement | Minor - P4 |  |
| WT-14775 | Investigate why ordering of import fixes libstdc++.so symbolisation | Backlog | Bug | Major - P3 |  |
| WT-14792 | 5.59% decrease in Update count in Variant ubuntu2004-perf-tests for Task perf-test-update-only-btree in Test update-only-btree.wtperf | Open | Build Failure | Major - P3 | perf-change-point |
| WT-14803 | Fix bugs and merge in smart testing skunkworks | Backlog | New Feature | Major - P3 |  |
| WT-14805 | timestamp_abort has an hour long stall during or after RTS while in wiredtiger_open | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-14811 | Add stat for total size of discarded pages | Backlog | Task | Major - P3 |  |
| WT-14815 | Enable the exclusion of static configuration values from WiredTiger metric cursor results | Backlog | Improvement | Minor - P4 |  |
| WT-14819 | Don't force full backups of the history store file for every incremental backup. | Open | Improvement | Major - P3 | backup-support |
| WT-14824 | Investigate whether we can compile standard libraries from v5 toolchain | Backlog | Improvement | Major - P3 |  |
| WT-14838 | Handle prepared rollback with rollback timestamp for page deleted structure | Open | Task | Major - P3 |  |
| WT-14839 | Improve documentation for wt verify -d mode | Open | Documentation | Minor - P4 |  |
| WT-14845 | failed: unit-test on amazon2-arm64 [wiredtiger-mongo-v7.0 @ 51cd3b74] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-14854 | failed: unit-test-extra-long on ubuntu2004-nonstandalone [wiredtiger @ 9cea8d8e] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14855 | Move block configure check inside __wti_blkcache_remove | Open | Technical Debt | Minor - P4 |  |
| WT-14856 | Fix TSAN warnings reported for ex_all | Open | New Feature | Major - P3 |  |
| WT-14859 | __wt_txn_timestamp_usage_check is comparing commit timestamp against previous update's durable timestamp | Backlog | Task | Major - P3 |  |
| WT-14863 | Assert read operation result in test/model | Backlog | Improvement | Minor - P4 |  |
| WT-14874 | Introduce timing_stress option into python testing | Backlog | Improvement | Major - P3 |  |
| WT-14889 | 5.00% decrease in Read count in Variant amazon2023-perf-tests-arm64 for Task perf-test-medium-btree in Test medium-btree.wtperf | Open | Build Failure | Major - P3 | perf-change-point |
| WT-14891 | 6.93% decrease in Read count in Variant ubuntu2004-perf-tests for Task perf-test-evict-btree in Test evict-btree.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-14892 | 9.53% decrease in Read count in Variant amazon2023-perf-tests-arm64 for Task perf-test-medium-btree in Test medium-btree.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-14893 | 6.26% decrease in Update count in Variant ubuntu2004-perf-tests for Task perf-test-update-large-record-btree in Test update-large-record-btree.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-14894 | 4.84% decrease in Read count in Variant amazon2023-perf-tests-arm64 for Task perf-test-evict-btree-1 in Test evict-btree-1.wtperf | Open | Build Failure | Major - P3 | perf-change-point |
| WT-14917 | failed: split-stress-test on ubuntu2004-arm64-nonstandalone [wiredtiger @ 65d9714f] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14923 | Coverity analysis defect 174896: Unintentional integer overflow | Open | Bug | Major - P3 | coverity, wiredtiger.develop |
| WT-14925 | Coverity analysis defect 174910: Structurally dead code | Backlog | Bug | Minor - P4 | coverity, wiredtiger.develop |
| WT-14933 | test/model prepare op - failed txn requires rollback | Backlog | Bug | Major - P3 |  |
| WT-14934 | test/model - failed txn requires rollback | Backlog | Bug | Major - P3 |  |
| WT-14936 | Checksum mismatch test_backup15 unit-test-macos on macos-1100 [v7.0] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-14940 | task-timed-out: unit-test-bucket03 on windows [wiredtiger @ 4247228a] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14942 | 10.21% decrease in Read count in Variant amazon2023-perf-tests-arm64 for Task perf-test-medium-btree-backup in Test medium-btree-backup.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-14948 | Investigate and implement connection->reconfigure expectations | Backlog | Task | Major - P3 |  |
| WT-14958 | Continue fixing ex_backup warnings | Open | Improvement | Major - P3 | code-quality |
| WT-14962 | Re-evaluate the need for S3 tests in evergreen | Backlog | Task | Major - P3 |  |
| WT-14966 | Abort on unexpected timestamp usage during __wti_btcur_evict_reposition (test/format) | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-14973 | CMake technical debt reduction | Open | Technical Debt | Minor - P4 |  |
| WT-14995 | test_autoclose fails with "argument 1 of type 'struct __wt_cursor *' is None" (6.0 and 7.0) | Backlog | Build Failure | Minor - P4 | BB-Tools |
| WT-15009 | task-timed-out: precise-checkpoint-stress-test on amazon2023-armv9-release-nonstandalone [wiredtiger @ 19f97c5f] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15013 | Track memory usage of constructing record with update chain | Backlog | Task | Major - P3 |  |
| WT-15018 | Update WiredTiger Release Notes for SPM-4331 | Open | Task | Major - P3 |  |
| WT-15019 | Update WiredTiger Release Notes for SPM-4332 | Open | Task | Major - P3 |  |
| WT-15020 | failed: format-stress-test-2 on amazon2023-armv9-release-nonstandalone [wiredtiger @ 376b8af7] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15021 | Automation failing to collect debug logs with error: [python3]: No such file or directory. | Backlog | Bug | Major - P3 |  |
| WT-15022 | Size of checkpoint cookie should be defined in terms of size of address cookie | Open | Bug | Major - P3 |  |
| WT-15023 | 14.90% increase in open_cursor_uncached_instructions in Variant amazon2023-perf-tests-arm64-only for Task cppsuite-api-instruction-count-benchmarks-default-perf in Test api_instruction_count_benchmarks | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-15024 | Extend crash testing framework with a configurable background thread for randomized crash points | Backlog | Epic | Major - P3 |  |
| WT-15029 | Abort when closing a file during shutdown returns an error code | Backlog | Task | Major - P3 |  |
| WT-15036 | test/model does not preserve db when hitting known_issue_exception | Open | Bug | Major - P3 |  |
| WT-15044 | cache_write_app_count and cache_write_app_time should not include the writes done by a checkpoint | Backlog | Bug | Major - P3 |  |
| WT-15061 | Add crash point before checkpoint txn commit | Open | Improvement | Major - P3 |  |
| WT-15063 | Doc update private key exposed in Evergreen task logs | Open | Bug | Major - P3 | security |
| WT-15066 | Improve the error message of WT_UNCOMMITTED_DATA | Open | Improvement | Major - P3 |  |
| WT-15069 | test/model should support crashpoints that result in recoverable checkpoints | Open | Bug | Major - P3 |  |
| WT-15073 | failed: format-stress-asan-ppc-test-1 on rhel8-ppc [wiredtiger @ 7a22f6dd] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-15077 | Update WiredTiger Release Notes for SPM-4349 | Open | Task | Major - P3 |  |
| WT-15078 | session_get_dhandle returning EBUSY leaves hanging handle | Open | Bug | Major - P3 |  |
| WT-15079 | Investigate how to best cursor open performance measurements | Backlog | Improvement | Major - P3 |  |
| WT-15083 | Automatically pull coredump into wiredtiger folder when spawning host | Open | Improvement | Major - P3 | Evergreen |
| WT-15084 | Run test/model with logging enabled on tables | Open | Improvement | Major - P3 |  |
| WT-15119 | Dynamically tune WT_EVICT_MODIFY_COUNT_MIN based on workload characteristics | Open | Task | Major - P3 | performance |
| WT-15127 | failed: format-stress-test-1 on amazon2023-stress-tests-armv9 [wiredtiger @ 660f6918] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15136 | Refactor __rec_upd_select to break it into smaller functions | Backlog | Task | Major - P3 |  |
| WT-15137 | Refactor __wti_rec_hs_insert_updates to break it into multiple functions | Backlog | Task | Major - P3 |  |
| WT-15145 | Compare metadata contents between a 4.4 and a 8.0 database | Open | Task | Major - P3 |  |
| WT-15149 | A checkpoint history store cursor cannot dump the history store content | Backlog | Bug | Major - P3 |  |
| WT-15152 | test_cursor19 fails with AssertionError: 1 != 3 in verify_value for modify operation on ubuntu2004 (7.0) | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15170 | Coverity analysis defect 175792: Resource leak | Backlog | Bug | Major - P3 | coverity, wiredtiger.develop |
| WT-15174 | Coverity analysis defect 175777: Logically dead code | Backlog | Bug | Major - P3 | coverity, wiredtiger.develop |
| WT-15185 | Explore compacting wt files via "punching a hole" in the file. | Open | Improvement | Minor - P4 |  |
| WT-15201 | Improve the transaction snapshot memory allocation | Backlog | Improvement | Major - P3 |  |
| WT-15218 | failed: make-check-test on macos-11 [wiredtiger-mongo-v6.0 @ 25a568e0] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15219 | Fix test_txn27 cache stuck dirty failure | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15243 | Bulk cursor and drop segmentation fault | Open | Bug | Major - P3 |  |
| WT-15245 | Consider applying bit encoding to other fields in address cookie. | Open | Improvement | Major - P3 |  |
| WT-15248 | Compilation issue missed in WiredTiger and caught by MongoDB tests | Backlog | Task | Major - P3 |  |
| WT-15250 | Test format replay_prepare_ts sets wrong prepared timestmap | Open | Bug | Major - P3 |  |
| WT-15257 | failed: unit-test-hook-tiered-s3 on ubuntu2004 [wiredtiger @ 9e551eae] | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-15258 | Investigate why compact causes system memory fragmentation | Open | Task | Major - P3 |  |
| WT-15264 | failed: unit-test-macos on macos-11 [wiredtiger-mongo-v6.0 @ 25a568e0] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15265 | dist/ script to check metadata-persisted config removal | Backlog | Improvement | Minor - P4 |  |
| WT-15282 | Rewrite compatibility test script in a sane language | Backlog | Improvement | Minor - P4 |  |
| WT-15295 | test_tiered16: AssertionError: Lists differ | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-15297 | test/format/RUNDIR.19 exited with status 127 for an unknown reason | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15304 | 5.49% decrease in Update count in Variant ubuntu2004-perf-tests for Task perf-test-update-large-record-btree in Test update-large-record-btree.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-15305 | 2.09% decrease in Blocks read in Variant ubuntu2004-perf-tests for Task prefetch-off-verify in Test microbenchmark_prefetch_off_verify.py | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-15312 | WT_SESSION::drop can incorrectly return EBUSY due to WT_UNCOMMITTED_DATA | Open | Bug | Major - P3 |  |
| WT-15323 | 10.83% decrease in Update count in Variant ubuntu2004-perf-tests for Task perf-test-long-500m-btree-50r50u-backup in Test 500m-btree-50r50u-backup.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-15324 | test/model fails on MacOS | Open | Bug | Major - P3 |  |
| WT-15325 | failed: unit-test-zstd on rhel80 [wiredtiger @ a2f96de5] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-15326 | failed: long-test on rhel80 [wiredtiger-mongo-v7.0 @ b8ef087e] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15332 | 12.07% decrease in Update count in Variant ubuntu2004-perf-tests for Task perf-test-update-delta-mix2 in Test update-delta-mix2.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-15333 | 9.27% decrease in Update count in Variant amazon2023-perf-tests-arm64 for Task perf-test-update-delta-mix1 in Test update-delta-mix1.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-15337 | Consider moving weakened standalone barriers to production | Open | Bug | Major - P3 |  |
| WT-15338 | TSAN: Remove suppression and annotation from standalone barriers when they are supported by TSAN | Open | Bug | Major - P3 |  |
| WT-15341 | failed: format-stress-test-1 on amazon2023-armv9-release-nonstandalone [wiredtiger @ 50e6d880] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15361 | failed: perf-cache-workload-dirty-trigger on ubuntu2004-perf-tests [wiredtiger @ 4135ee25] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-15365 | failed: format-failure-configs-test on ubuntu2004 [wiredtiger @ 1d838d19] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15367 | failed: unit-test-extra-long on amazon2023-armv9-release-nonstandalone [wiredtiger @ de307e88] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15400 | Clarify usage of WT_PAGE_COMPRESSED flag in block_cache/block_io.c | Open | Improvement | Minor - P4 |  |
| WT-15403 | Ensure we edit the pr summary before merging | Backlog | Bug | Major - P3 |  |
| WT-15435 | failed: compatibility-test-for-newer-releases on compatibility-tests [wiredtiger @ bd06c967] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15436 | failed: cppsuite-default-all on ubuntu2004-ubsan [wiredtiger @ 544e3a58] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15442 | Make the semantics of WT_ERR_MSG/WT_RET_MSG consistent with WT_ERR/WT_RET | Backlog | Improvement | Major - P3 |  |
| WT-15458 | task-timed-out: skiplist-stress-test on rhel8-zseries [wiredtiger @ c8c100f8] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15468 | test_txn06.py assertion failure | Open | Build Failure | Major - P3 | BB-Tools |
| WT-15469 | Log server hangs in test_backup13.py | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-15470 | failed: azure-gcp-tiered-catch2-unittest-test on ubuntu2004 [wiredtiger @ 86339af2] | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-15481 | test_txn27.py no assertion raised | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15482 | test_checkpoint_4_mixed_sweep illegal value (v7.0) | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15490 | Live restore cannot find WT_LR_DEST/file1.txt | Open | Build Failure | Major - P3 | BB-Tools |
| WT-15496 | snappy_compress crash | Open | Build Failure | Major - P3 | BB-Tools |
| WT-15537 | Check usage of backwards-compatibility flag in compatibility tests | Backlog | Task | Minor - P4 |  |
| WT-15538 | Investigate slow eviction behavior when updates ratio is high | Open | Task | Major - P3 | e-digest |
| WT-15542 | s_all should run test/format/config.sh | Backlog | Improvement | Major - P3 |  |
| WT-15557 | Use check and reset macro to refactor existing code | Open | Task | Minor - P4 | neweng |
| WT-15573 | Add metric for cache dirty fill ratio by committed vs uncommitted pages | Open | Improvement | Major - P3 |  |
| WT-15574 | Dynamically allocate WT_SESSIONs internally for session_max | Backlog | Improvement | Major - P3 |  |
| WT-15580 | failed: unit-test-bucket02 on ubuntu2004 [wiredtiger-mongo-v6.0 @ 25a568e0] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15581 | Make s_all a prerequisite for evergreen PR testing to save resources and reduce wasted effort. | Backlog | Improvement | Major - P3 |  |
| WT-15584 | Python tests produce `RuntimeWarning` when run in parallel | Open | Bug | Major - P3 |  |
| WT-15586 | Consider introducing an "admin mode" to restrict access to WT internals through external API | Open | Task | Major - P3 |  |
| WT-15590 | 28.62% decrease in Cursor Joins in Variant amazon2023-perf-tests-arm64 for Task bench-wt2853-perf-test-col in Test wt2853_perf_col | Open | Build Failure | Major - P3 | perf-change-point |
| WT-15604 | task-timed-out: spinlock-gcc-test | Open | Build Failure | Major - P3 | BB-Tools |
| WT-15611 | stress.hs_sweep not supported in backward compatibility mode | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15613 | block_cache not supported in backward compatibility mode | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15633 | Combine btree->ckpt_timestamp and btree->prune_timestamp into one variable or a union | Open | Task | Minor - P4 |  |
| WT-15635 | Make Catch2 test_session_config.cpp work with compiled configuration | Backlog | Task | Major - P3 |  |
| WT-15646 | Avoid creating a dhandle when the resource doesn't exist | Backlog | Task | Major - P3 |  |
| WT-15664 | failed: compatibility-test-suite on compatibility-tests-daily [wiredtiger @ e9250570] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15673 | Add histogram statistics to improve understanding of tree shape | Open | Improvement | Major - P3 |  |
| WT-15702 | Investigate a solution to handle repeated failure to force evict a page | Open | Task | Major - P3 |  |
| WT-15703 | test_rollback_to_stable39 hs_removed is not equal to 0 (7.0) | Open | Build Failure | Major - P3 | BB-Tools |
| WT-15708 | TSAN Warning: unprotected access to log->sync_lsn.l.file | Open | Task | Major - P3 |  |
| WT-15714 | s_all doesn't generate api_data config after finding illegal comment | Backlog | Task | Major - P3 | neweng |
| WT-15752 | Continue fixing data races in statistic | Open | Task | Major - P3 |  |
| WT-15754 | Consider using relaxed atomic RMW and CAS for stats | Open | Task | Major - P3 |  |
| WT-15755 | Consider using CAS for calculating max statistic values | Open | Task | Major - P3 |  |
| WT-15762 | Reduce contention b/w sweep server and checkpoint prepare by skipping sweep during active checkpoint | Open | Bug | Major - P3 |  |
| WT-15774 | Identify the process that opened the file on an EBUSY error under Windows | Open | Improvement | Major - P3 |  |
| WT-15791 | Use ***UntilSuccess() in python tests rather than direct calls. | Open | Improvement | Minor - P4 | neweng |
| WT-15796 | Investigate if we can track how often a sub system fails | Open | Task | Major - P3 |  |
| WT-15802 | Number the lines in the error log output | Backlog | Improvement | Major - P3 |  |
| WT-15804 | Enhance s_all to generate warnings if we forget to unlock the ref | Backlog | Task | Major - P3 |  |
| WT-15805 | Adjust "Building and installing" docs to match reality | Backlog | Improvement | Minor - P4 |  |
| WT-15809 | Numerous test tasks compile their binaries instead of using artefacts from the previous `compile` task | Open | Bug | Major - P3 |  |
| WT-15811 | s_string should spell check coding style and other text files | Backlog | Improvement | Major - P3 |  |
| WT-15816 | Opening a stable cursor can return both ENOENT and WT_NOTFOUND | Open | Technical Debt | Major - P3 |  |
| WT-15817 | Introduce 60/120 minute test/format runs to evergreen | Backlog | Task | Major - P3 |  |
| WT-15821 | Error semantic for stat of checkpoint_cleanup_pages_obsolete_tw | Open | Bug | Major - P3 |  |
| WT-15823 | Allow WT to rollback read transactions in non-standalone builds | Backlog | Improvement | Minor - P4 |  |
| WT-15825 | Add check for ticket citations in WT sources | Backlog | Improvement | Minor - P4 |  |
| WT-15847 | Use subprocess.exec in place of shell.exec in evergreen.yml | Backlog | Technical Debt | Minor - P4 |  |
| WT-15855 | Create macro for safe casting of dhandles | Open | Improvement | Major - P3 |  |
| WT-15862 | Ensure python test suite properly cleans up when reassigning variables | Backlog | Bug | Major - P3 |  |
| WT-15937 | failed: format-stress-test-4 on amazon2023-stress-tests-armv9 [wiredtiger @ 545d3231] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-15943 | test_calc_modify fails for random numbers other than 42 | Open | Bug | Major - P3 |  |
| WT-15944 | Support verify dump pages on metadata table | Backlog | Task | Major - P3 |  |
| WT-15946 | Extend wiredtiger b-tree data source statistics | Open | Task | Minor - P4 |  |
| WT-15961 | Introduce thread additional thread-safe interfaces for statistic | Open | Task | Major - P3 |  |
| WT-15980 | Deprecate the checksum file_config option which defaults to on. | Open | Improvement | Major - P3 |  |
| WT-16007 | Investigate if a checkpoint cursor can be opened when a checkpoint is running | Backlog | Task | Major - P3 |  |
| WT-16016 | 14.18% decrease in Read count in Variant amazon2023-perf-tests-arm64 for Task perf-test-chunk-cache in Test chunk-cache-reads.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-16022 | Segfault when importing a table | Open | Bug | Major - P3 |  |
| WT-16030 | Coverity analysis defect 169519: Check of thread-shared field evades lock acquisition | Open | Bug | Major - P3 | coverity, wiredtiger.develop |
| WT-16031 | Investigate Slow Eviction Stats in tests | Open | Task | Major - P3 |  |
| WT-16034 | Coverity analysis defect 178019: Variable copied when it could be moved | Open | Bug | Trivial - P5 | coverity, wiredtiger.develop |
| WT-16057 | Allow passing in a deadline when opening a backup cursor | Open | Improvement | Major - P3 |  |
| WT-16058 | failed: format-stress-test-2 on ubuntu2004-stress-tests-arm64 [wiredtiger-mongo-v7.0 @ 4d417589] | Backlog | Build Failure | Major - P3 | BB-Tools, dc, na-mdb |
| WT-16062 | 11.68% decrease in Read count in Variant amazon2023-perf-tests-arm64 for Task perf-test-modify-read-btree in Test modify-read-btree.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-16063 | 6.65% decrease in Read count in Variant amazon2023-perf-tests-arm64 for Task perf-test-modify-read-btree in Test modify-read-btree.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-16066 | Remove pytest_parallel | Backlog | Build Failure | Major - P3 |  |
| WT-16067 | Investigate parallel tests execution with sanitizers | Open | Build Failure | Major - P3 |  |
| WT-16071 | Investigate if WT_SESSION::reconfigure has to always reset open cursors | Open | Improvement | Major - P3 |  |
| WT-16073 | 5.05% increase in default_next_nanoseconds_90th_percentile in Variant amazon2023-perf-tests-arm64 for Task cppsuite-bounded-cursor-perf-stress-perf in Test bounded_cursor_perf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-16074 | 18.61% increase in Update count under sec in Variant amazon2023-perf-tests-arm64 for Task perf-cache_dirty_trigger_read-90_write-10 in Test cache_dirty_trigger_read-90_write-10.py | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-16076 | 5.21% decrease in Insert count in Variant amazon2023-perf-tests-arm64 for Task perf-test-update-btree in Test update-btree.wtperf | Open | Build Failure | Major - P3 | perf-change-point |
| WT-16079 | 28.52% decrease in Update count under sec in Variant ubuntu2004-perf-tests for Task perf-cache_update_trigger_read-90_write-10 in Test cache_update_trigger_read-90_write-10.py | Open | Build Failure | Major - P3 | perf-change-point |
| WT-16083 | Fix newly detected ASAN warnings | Open | Build Failure | Major - P3 | expedite |
| WT-16090 | Remove wt2246_col_append/main.c | Open | Improvement | Major - P3 |  |
| WT-16091 | Allow compression adjustment for 16 KB leaf pages | Backlog | Improvement | Major - P3 |  |
| WT-16093 | 3.20% increase in timestamp_transaction_uint_instructions in Variant amazon2023-perf-tests-arm64-only for Task cppsuite-api-instruction-count-benchmarks-default-perf in Test api_instruction_count_benchmarks | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-16094 | 47.57% increase in set_bounds_non-compiled_nanoseconds_90th_percentile in Variant amazon2023-perf-tests-arm64 for Task cppsuite-bounded-cursor-perf-stress-perf in Test bounded_cursor_perf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-16096 | Investigate and reduce excessive "read timestamp less than oldest timestamp" WT logs | Open | Task | Major - P3 |  |
| WT-16099 | s_all check for WT_DHANDLE_CLEAR macro | Backlog | Task | Minor - P4 |  |
| WT-16100 | failed: perf-test-medium-btree-backup on amazon2023-perf-tests-arm64 [wiredtiger @ 32d5a819] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16107 | Investigation into cache eviction in QE | Open | Task | Major - P3 |  |
| WT-16124 | Make s_outdated_fixmes work in pull request testing | Open | Task | Minor - P4 | devprod |
| WT-16128 | test_wt8057_compact_stress did not catch compact events | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16142 | Update upgrading.dox, to clarify VLCS support for the next WiredTiger release. | Open | Improvement | Major - P3 |  |
| WT-16144 | Refactor CppSuite counters to re-enable MacOS builds | Open | Improvement | Major - P3 | neweng |
| WT-16146 | Clean up inconsistent metadata for tiered/complex tables during log recovery | Backlog | Improvement | Major - P3 |  |
| WT-16151 | Dump memory region containing "illegal value" whenever it's possible | Open | Improvement | Major - P3 |  |
| WT-16152 | Update WiredTiger Release Notes for SPM-4479 | Open | Task | Major - P3 |  |
| WT-16154 | failed: spinlock-pthread-adaptive-test on ubuntu2004 [wiredtiger-mongo-v7.0 @ 2b5317b5] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16158 | Avoid checking deleted entries for a stable constituent | Backlog | Build Failure | Major - P3 |  |
| WT-16164 | Review usages of __wt_config_merge to ensure they correctly pass a not sparse array. | Backlog | Technical Debt | Major - P3 |  |
| WT-16171 | Coverity analysis defect 183948: Check of thread-shared field evades lock acquisition | Open | Bug | Major - P3 | coverity, wiredtiger.develop |
| WT-16178 | Move test_live_restore to use the CppSuite database model instead of it's own one | Open | Technical Debt | Minor - P4 |  |
| WT-16189 | failed: spinlock-pthread-adaptive-test on ubuntu2004 [wiredtiger @ 06ab2460] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16215 | Do not use meta tracking during connection recovery | Backlog | Bug | Major - P3 |  |
| WT-16233 | Review WT_CONFIG_ITEM usages to ensure initialization prior to calling `__wt_config` API | Backlog | Technical Debt | Major - P3 |  |
| WT-16234 | Mark a dhandle as outdated only when the checkpoint change | Backlog | Improvement | Major - P3 |  |
| WT-16235 | Compile wiredtiger external header file under C++ to ensure compliance | Backlog | Improvement | Major - P3 |  |
| WT-16240 | Investigate diagnostic gaps for crash loops | Open | Task | Major - P3 |  |
| WT-16241 | Support decoding MongoDB index entries in wt_binary_decode | Open | Task | Major - P3 |  |
| WT-16264 | Test format timeout | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16266 | Format error: Compaction halted at data handle by eviction pressure. Returning EBUSY | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16270 | mirror error on rhel8-zseries [wiredtiger @ 0f0e6505] | Open | Build Failure | Major - P3 | dc, na-mdb |
| WT-16271 | failed: catch2-assertions on ubuntu2004 [wiredtiger @ c48c9402] | Open | Build Failure | Major - P3 | BB-Tools, Test |
| WT-16273 | format run more than 15 minutes past the maximum time | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16388 | Broken link for json artifacts in Perf tests | Backlog | Bug | Major - P3 |  |
| WT-16412 | failed: unit-test-hook-tiered-with-delays on ubuntu2004 [wiredtiger @ 15a68dfb] | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-16419 | failed: format-stress-test-tsan on ubuntu2004-tsan [wiredtiger @ 12af855a] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16420 | task-timed-out: csuite-timestamp-abort-test-s3 on ubuntu2004 [wiredtiger-mongo-v8.2 @ bf55d99e] | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-16421 | Create a test to cover invalid path involving checkpoint cursors and bulk cursors | Backlog | Task | Major - P3 |  |
| WT-16429 | Support GetTableAtLSN protobuf format in wt_binary_decode | Open | Improvement | Major - P3 |  |
| WT-16440 | Write Performance Reconciliation Efficiency - Minimize Full Page Writes | Backlog | Task | Major - P3 |  |
| WT-16441 | Write Performance Reconciliation Efficiency - Evict re-splitting pages | Open | Task | Major - P3 |  |
| WT-16445 | Write Performance Faster Checkpoint - Preflush pages for checkpoint | Open | Task | Major - P3 |  |
| WT-16446 | Write Performance Smaller Page Size - Enable prefetch by default | Backlog | Task | Major - P3 |  |
| WT-16447 | Write Performance - Checkpoint Frequency | Open | Task | Major - P3 |  |
| WT-16449 | Fix 'TypeError: 'staticmethod' object is not callable' error in wttest.py for Mac/OSX | Backlog | Bug | Major - P3 |  |
| WT-16457 | cursor open failures after switching statistics from size to fast in SERVER-113418 | Open | Bug | Major - P3 |  |
| WT-16469 | test/model hits rollback_to_stable illegal with active transactions | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16471 | failed: model-test-long-with-coverage on ubuntu2004 [wiredtiger @ eec5e849] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16473 | failed: workgen-test-prepare_stress on amazon2023-armv9 [wiredtiger @ 16020c3f] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16484 | Enable field introspection in WiredTiger | Backlog | Improvement | Major - P3 |  |
| WT-16488 | failed: format-stress-test-2 on amazon2023-armv9-nonstandalone [wiredtiger @ 3ca2aa25] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16491 | Create documentation for different stages in the recovery phase | Open | Documentation | Major - P3 |  |
| WT-16498 | Data format change to allow writing prepared fast truncate to the disk | Open | Task | Major - P3 | wt_data_format |
| WT-16512 | Make the page header length extensible | Open | Task | Major - P3 | wt_data_format |
| WT-16517 | failed: unit-test-extra-long on rhel80 [wiredtiger @ fd829370] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16519 | failed: unit-test-macos on macos-14-arm64 [wiredtiger @ fd352b48] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16524 | Make the implementation of pl_abandon_checkpoint mandatory | Open | Task | Major - P3 | neweng |
| WT-16527 | Implement start_lsn for trim command | Open | Task | Major - P3 |  |
| WT-16534 | Implement cold read | Open | New Feature | Major - P3 |  |
| WT-16540 | When we reconfigure the iocapacity, do not destroy the iocapacity thread, as destroying the thread will trigger a mongod crash. | Open | Bug | Major - P3 |  |
| WT-16551 | failed: unit-test-macos on macos-1100 [wiredtiger-mongo-v7.0 @ 442285cb] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16558 | Create extension plugin for dhandle stress testing and benchmarks | Open | Improvement | Major - P3 |  |
| WT-16576 | Improve t2 cache state reporting | Open | Task | Major - P3 |  |
| WT-16581 | failed: format-stress-test-1 on amazon2023-armv9-release-nonstandalone [wiredtiger @ bb6189df] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16582 | failed: make-check-test on ubuntu2004-msan [wiredtiger-mongo-v7.0 @ 442285cb] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16584 | failed: make-check-test on macos-1100 [wiredtiger-mongo-v7.0 @ 442285cb] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16585 | failed: format-stress-test-2 on amazon2023-stress-tests-arm64 [wiredtiger @ 3acda846] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16590 | Evaluate Evergreen task tag vs task regex for configuring pull request tests | Open | Improvement | Major - P3 |  |
| WT-16593 | Coverity analysis defect 185719: Explicit null dereferenced | Open | Bug | Major - P3 | coverity, wiredtiger.develop |
| WT-16594 | Coverity analysis defect 185721: Dereference after null check | Open | Bug | Major - P3 | coverity, wiredtiger.develop |
| WT-16596 | Coverity analysis defect 185631: Redundant test/check | Open | Bug | Trivial - P5 | coverity, wiredtiger.develop |
| WT-16607 | Create a test for very large table sizes | Open | Workload | Major - P3 |  |
| WT-16614 | Investigate scaling WT eviction walk target for large cache workloads | Open | Task | Major - P3 |  |
| WT-16615 | Add wt_binary_decode tests to evergreen | Open | Task | Major - P3 |  |
| WT-16616 | Remove opts arg from parsing functions in binary decode | Open | Task | Major - P3 |  |
| WT-16617 | Separate printing from file object parsing in binary decode | Open | Task | Major - P3 |  |
| WT-16618 | failed: spinlock-gcc-test __wt_lsm_tree_drop assertion failed on amazon2-arm64 [wiredtiger-mongo-v7.0 @ 442285cb] | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-16620 | failed: unit-test-macos test_cursor_random on macos-14-arm64 [wiredtiger @ ef8d9217] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16621 | Make develop documentation more prominent in search results | Open | Improvement | Major - P3 |  |
| WT-16637 | Create Metric to Track Corrupted Pages Read from PageServer | Open | Task | Major - P3 | ds_durability_high_risk, ds_durability_mitigation |
| WT-16640 | The data_handle_locust workload does not cause unavailability on Atlas | Open | Bug | Major - P3 | reproducer-no-unavailability |
| WT-16647 | Read and log information about orphaned blocks | Open | Task | Major - P3 |  |
| WT-16651 | Mirror mismatch on 7.0 between VLCS and FLCS | Open | Build Failure | Major - P3 | BB-Tools, na-mdb |
| WT-16653 | Clarification needed on initialization of checkpoint_timestamp and oldest_timestamp in WiredTiger | Backlog | Task | Major - P3 |  |
| WT-16665 | Investigate eviction queue resize based on efficiency | Open | Task | Major - P3 |  |
| WT-16666 | Refactor __rec_write_wrapup callers to __wt_btree_decrease_size. | Open | Technical Debt | Major - P3 |  |
| WT-16672 | Investigate if prefetch should be tested in test/model | Open | Task | Minor - P4 |  |
| WT-16680 | 11.39% increase in Database Size (in bytes) in Variant amazon2023-perf-tests-arm64 for Task perf-test-mongodb-oplog in Test mongodb-oplog.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-16682 | Assess disk fragmentation in the fleet | Open | Epic | Major - P3 | dta-persistence-slo |
| WT-16683 | Investigate fragmentation visualisation tools | Open | Task | Major - P3 |  |
| WT-16684 | Investigate how to get observable metrics from the fleet regarding disk fragmentation | Open | Task | Major - P3 |  |
| WT-16688 | Extend prefetch testing | Open | Epic | Major - P3 |  |
| WT-16689 | Measure code coverage related to the prefetch feature | Open | Task | Major - P3 |  |
| WT-16690 | Check with other teams how they can benefit from prefetch | Open | Task | Major - P3 |  |
| WT-16697 | Investigation of spurious errors related to the block manager | Open | Epic | Major - P3 |  |
| WT-16699 | Improve checkpoint observability | Open | Epic | Major - P3 | dta-persistence-slo |
| WT-16700 | Create a dashboard to track checkpoint cleanup performance | Open | Sub-task | Major - P3 |  |
| WT-16702 | test/format read_op  conflict between concurrent operations error | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16710 | Add compact configuration to override the 90/10 space requirement | Open | Task | Minor - P4 |  |
| WT-16712 | test/format abort failure in reconcilliation | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16713 | Create in-WiredTiger tests for victim block cache | Backlog | Task | Major - P3 |  |
| WT-16717 | stable_timestamp TSAN data race warning in __txn_set_rollback_timestamp | Open | Bug | Major - P3 |  |
| WT-16718 | (HELP-86942 Postmortem): Visa - WT Dirty cache going beyond threshold | Open | Task | Major - P3 |  |
| WT-16722 | Cache performance fluctuate investigation | Backlog | Task | Major - P3 |  |
| WT-16723 | 57.67% increase in Cache updates trigger in Variant ubuntu2004-perf-tests for Task perf-cache-workload-update-trigger in Test cache_workload_update_trigger.py | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-16726 | Add tests to test_decode_log_mongodb.py | Open | Task | Major - P3 | neweng |
| WT-16730 | Refactor wt_binary_decode.py to improve modularity maintainability | Open | Improvement | Major - P3 |  |
| WT-16740 | failed: format-stress-test-tsan on ubuntu2004-tsan [wiredtiger @ e894ed61] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16745 | Investigate eviction queue usage when the queue is frequently empty | Open | Task | Major - P3 |  |
| WT-16754 | Review whether all checks are needed when the dhandle is exclusively taken in __wt_page_can_evict | Backlog | Task | Minor - P4 |  |
| WT-16757 | Investigate sweep03 error when queuing drops | Open | Task | Major - P3 |  |
| WT-16759 | Failed: azure-gcp-tiered-catch2-unittest-test on ! Ubuntu 20.04 (Non-standalone) [WiredTiger (develop) @ a7e3d2c4] | Open | Build Failure | Minor - P4 |  |
| WT-16762 | Pretty print the key string when decoding the history store | Open | Task | Minor - P4 |  |
| WT-16768 | Implement parallel checkpoint | Open | Epic | Major - P3 |  |
| WT-16777 | Update WiredTiger Release Notes for SPM-4590 | Open | Task | Major - P3 |  |
| WT-16778 | Ensure we use atomic operations to read/write global durable timestamps | Open | Task | Major - P3 |  |
| WT-16779 | __curfile_close vs __thread_set_name data race | Backlog | Build Failure | Major - P3 |  |
| WT-16787 | Re-evaluate the value that session->name brings | Backlog | Task | Minor - P4 |  |
| WT-16788 | Create dashboard(s) to find out which checkpoint states mostly impact long checkpoints | Open | Sub-task | Major - P3 |  |
| WT-16791 | Create a dashboard on background compaction usage in the fleet | Open | Task | Major - P3 |  |
| WT-16794 | Investigate a way to query B-tree metrics from atlas | Open | Task | Major - P3 |  |
| WT-16796 | Create a new stat to capture the number of extents | Open | Task | Major - P3 |  |
| WT-16801 | Review POC cursor read path interaction with fast truncate structure | Backlog | Task | Major - P3 |  |
| WT-16807 | Investigate AF with an inconclusive fix | Open | Task | Major - P3 |  |
| WT-16816 | Reduce disk fragmentation | Open | Epic | Major - P3 |  |
| WT-16826 | __wt_col_search, 186: WiredTiger assertion failed: 'base > 0' causing __wt_abourt | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-16829 | Use AI to create some illustrations how prepared transaction works | Backlog | Task | Major - P3 |  |
| WT-16834 | Add regression test for the table IDs conflict | Open | Task | Major - P3 |  |
| WT-16836 | Investigate ways for comprehensive testing for Table IDs conflicts | Open | Task | Major - P3 |  |
| WT-16838 | Add software prefetch pipeline to WiredTiger B-tree search and cursor reopen path | Open | Task | Major - P3 |  |
| WT-16857 | Possible history store cursor bug found by in test_drop01 | Open | Bug | Major - P3 |  |
| WT-16859 | test_prepare28 fails when closing cursors | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16866 | failed: format-stress-test-4-nonstandalone on ubuntu2004-stress-tests [wiredtiger-mongo-v7.0 @ 8d495472] | Open | Build Failure | Critical - P2 | BB-Tools |
| WT-16874 | failed: spinlock-pthread-adaptive-test on amazon2023-release-arm64 [wiredtiger @ 95c795ef] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16886 | failed: bench-tiered-push-pull-s3 on ubuntu2004 [wiredtiger-mongo-v7.0 @ 8d495472] | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-16898 | task-timed-out: csuite-timestamp-abort-test-s3 on ubuntu2004 [wiredtiger-mongo-v8.0 @ 28e89bb6] | Open | Build Failure | Minor - P4 | BB-Tools |
| WT-16900 | 3.38% increase in cursor_modify_instructions in Variant amazon2023-perf-tests-arm64-only for Task cppsuite-api-instruction-count-benchmarks-default-perf in Test api_instruction_count_benchmarks | Backlog | Build Failure | Minor - P4 | perf-change-point |
| WT-16902 | Documentation Updates | Backlog | Task | Major - P3 |  |
| WT-16905 | WT hangs on implicit read-uncommitted search after modify update | Open | Bug | Major - P3 |  |
| WT-16908 | s_test_suite_no_executable fails in fast mode on older branches | Backlog | Task | Minor - P4 |  |
| WT-16909 | Update WiredTiger Release Notes for SPM-4630 | Open | Task | Major - P3 |  |
| WT-16923 | Test coverage for dirty bytes stat in checkpoint progress messages | Backlog | Task | Major - P3 | neweng |
| WT-16924 | Add checkpoint estimation logs | Open | Task | Major - P3 |  |
| WT-16927 | Implement a mechanism in wt_verify to fix WiredTiger's metadata if it finds checkpoint size discrepancy | Open | Task | Major - P3 |  |
| WT-16928 | failed: make-check-test on amazon2023-armv9-nonstandalone [wiredtiger-mongo-v8.2 @ dab48376] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16930 | TSAN race in __wti_log_slot_activate | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16933 | Assertion failure during RTS checkpoint after WT-16698 backport on 7.0 | Backlog | Bug | Major - P3 |  |
| WT-16937 | Enable debug_mode.cursor_copy in WT ASAN MSAN UBSAB testing | Open | Task | Major - P3 |  |
| WT-16963 | ooo key detected on macos-1100 [wiredtiger-mongo-v7.0 @ 8d495472] | Open | Build Failure | Critical - P2 | BB-Tools |
| WT-16964 | Read checksum error test_wt8246_compact_rts_data_correctness macos-1100 [v7.0] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-16973 | Scrub evict clean pages with updates to avoid needing to re-read them from storage | Open | Task | Major - P3 |  |
| WT-16983 | Assertion failure hit on checkpoint size | Open | Bug | Major - P3 |  |
| WT-16989 | Rethink s_outdated_fixmes | Backlog | Task | Major - P3 |  |
| WT-16991 | Redundant error handling in thread creation and destruction functions | Backlog | Technical Debt | Major - P3 |  |
| WT-16996 | Upgrade CI machines from Ubuntu 20.04 | Open | Task | Major - P3 |  |
| WT-16997 | Investigate the needs for prefetch performance testing | Open | Task | Major - P3 |  |
| WT-16999 | test_checkpoint: cursor->next() returns WT_PREPARE_CONFLICT [wiredtiger @ 518380a1] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-17000 | Investigate alternatives for bytes_total underflow handling in __wt_btree_decrease_size | Open | Task | Major - P3 |  |
| WT-17007 | 12.33% decrease in Read count in Variant amazon2023-perf-tests-arm64 for Task perf-test-evict-btree in Test evict-btree.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-17009 | failed: unit-test-bucket11 on windows-64 [wiredtiger-mongo-v7.0 @ eaa187c8] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-17012 | Adopt clang type-aware memory allocation instead of suppressing compilation warnings | Backlog | Task | Major - P3 |  |
| WT-17015 | Redesign checkpoint eviction threshold configuration API to avoid reconfig lock timeouts | Backlog | Task | Major - P3 |  |
| WT-17017 | Skip block cache if data being evicted was read by read_once cursor | Backlog | Improvement | Major - P3 |  |
| WT-17024 | 20.29% decrease in Cache dirty trigger in Variant amazon2023-perf-tests-arm64 for Task perf-cache-workload-dirty-trigger in Test cache_workload_dirty_trigger.py | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-17027 | Make __wt_spin_lock reentrant, and get rid of lock flags | Open | Improvement | Major - P3 |  |
| WT-17036 | 22.35% increase in set_bounds_compiled_nanoseconds_90th_percentile in Variant ubuntu2004-perf-tests for Task cppsuite-bounded-cursor-perf-stress-perf in Test bounded_cursor_perf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-17037 | 3.68% decrease in Read count in Variant ubuntu2004-perf-tests for Task perf-test-multi-btree-zipfian in Test multi-btree-zipfian-workload.wtperf | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-17039 | 128-bit integers support | Backlog | New Feature | Minor - P4 |  |
| WT-17047 | Update C++ standard to C++23 or newer | Open | Improvement | Minor - P4 |  |
| WT-17051 | Placeholder ticket for WT observablity work | Open | New Feature | Major - P3 |  |
| WT-17064 | Update cache usage accounting for in memory pages | Open | Task | Major - P3 |  |
| WT-17065 | Reserve cache usage for the cache used by the hash table | Open | Task | Major - P3 |  |
| WT-17082 | Re-read the page image from disk if page decoding fails | Backlog | Task | Major - P3 |  |
| WT-17083 | Add basic page format validation checks closer to page read, if practical | Backlog | Task | Major - P3 |  |
| WT-17100 | failed: format-stress-sanitizer-test-4 on ubuntu2004-stress-tests [wiredtiger-mongo-v7.0 @ eaa187c8] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-17102 | 33.82% decrease in Cache dirty trigger in Variant ubuntu2004-perf-tests for Task perf-cache-workload-dirty-trigger in Test cache_workload_dirty_trigger.py | Backlog | Build Failure | Major - P3 | perf-change-point |
| WT-17106 | Add cache leaf stat to T2 | Backlog | Task | Major - P3 | t2 |
| WT-17109 | Factor out common snapshot clone logic into internal helper | Backlog | Task | Major - P3 |  |
| WT-17111 | failed: azure-gcp-tiered-catch2-unittest-test on ubuntu2004 [wiredtiger-mongo-v8.2 @ 6cd77258] | Backlog | Build Failure | Minor - P4 | BB-Tools |
| WT-17136 | 9.99% decrease in cursor_update_instructions in Variant amazon2023-perf-tests-arm64-only for Task cppsuite-api-instruction-count-benchmarks-default-perf in Test api_instruction_count_benchmarks | Open | Build Failure | Major - P3 | expedite, perf-change-point |
| WT-17137 | failed: format-failure-configs-test on amazon2023-arm64 [wiredtiger @ 12875d02] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-17147 | Investigation: Add diagnostics/debugging for test/format failures to differentiate same data mismatch failures | Backlog | Task | Major - P3 |  |
| WT-17149 | Data examination tooling | Open | Epic | Major - P3 |  |
| WT-17150 | Read Path | Open | Story | Major - P3 |  |
| WT-17151 | Write Path | Open | Story | Major - P3 |  |
| WT-17153 | Define all the work that needs to be done to comply with the new workflow | Backlog | Sub-task | Major - P3 |  |
| WT-17155 | Define an option to redact | Backlog | Sub-task | Major - P3 |  |
| WT-17156 | Define the workflow | Backlog | Sub-task | Major - P3 |  |
| WT-17157 | Proposal to introduce error code levels for WT_TRET | Backlog | Task | Minor - P4 |  |
| WT-17165 | Improve CI log clarity and address spurious failures in s_all, s_export, and s_evergreen scripts | Backlog | Task | Minor - P4 |  |
| WT-17167 | Address upgrade/downgrade for new connection config options | Open | Task | Major - P3 |  |
| WT-17168 | Investigate if we can replace the spin lock to read write lock | Open | Task | Major - P3 |  |
| WT-17172 | POC: Implement intermediate flushes in bulk cursor load to bound peak memory usage | Open | Task | Major - P3 |  |
| WT-17175 | Expose lightweight stat for approximate leaf page count per table | Open | New Feature | Major - P3 |  |
| WT-17179 | Improve WT Compatibility Tests | Backlog | Epic | Major - P3 |  |
| WT-17180 | Improve local compatibility test experience by supporting specific release series selection | Backlog | Task | Major - P3 |  |
| WT-17181 | Ensure compatibility test coverage for minor releases is on par with major releases | Open | Task | Major - P3 |  |
| WT-17182 | test_update_obsolete_short_chain stat assertion failure self.assertGreater(removed_after_checkpoint, removed_after_second) | Open | Build Failure | Major - P3 | BB-Tools |
| WT-17190 | During GC, verify older updates against the HS | Open | Task | Major - P3 |  |
| WT-17193 | Limit prune timestamp eviction eligibility check to pages under the memory page max threshold | Open | Task | Major - P3 |  |
| WT-17199 | Investigate long running checkpoints on selected clusters using FTDC analysis | Open | Sub-task | Major - P3 |  |
| WT-17200 | Update dashboard to consume new checkpoint metrics for improved observability | Backlog | Sub-task | Major - P3 |  |
| WT-17201 | Investigate feasibility of automating long running checkpoint analysis in the fleet | Open | Sub-task | Major - P3 |  |
| WT-17202 | task-timed-out: unit-test on amazon2023-arm64 [wiredtiger @ 0e4ca779] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-17209 | Bulk insert flush — POC: add infrastructure skeleton | Open | Task | Major - P3 |  |
| WT-17210 | Bulk insert flush — POC: rough reconcile suspend/resume and rightmost path rebuild | Open | Task | Major - P3 |  |
| WT-17211 | Bulk insert flush — POC: rough orchestrator and end-to-end smoke test | Open | Task | Major - P3 |  |
| WT-17212 | Bulk insert flush — add configuration params, statistics, and harden enum/flag infrastructure | Open | Task | Major - P3 |  |
| WT-17213 | Bulk insert flush — implement production reconcile suspend/resume | Open | Task | Major - P3 |  |
| WT-17214 | Bulk insert flush — implement production rightmost path rebuild | Open | Task | Major - P3 |  |
| WT-17215 | Bulk insert flush — implement production __wt_bulk_flush() orchestrator | Open | Task | Major - P3 |  |
| WT-17216 | Bulk insert flush — add functional test suite (Python suite) | Open | Task | Major - P3 |  |
| WT-17217 | Bulk insert flush — add crash recovery tests | Open | Task | Major - P3 |  |
| WT-17218 | Bulk insert flush — cppsuite stress test: concurrent bulk loads with background checkpoint | Open | Task | Major - P3 |  |
| WT-17219 | Bulk insert flush — cppsuite stress test: crash injection during flush | Open | Task | Major - P3 |  |
| WT-17220 | Bulk insert flush — cppsuite stress test: memory pressure | Open | Task | Major - P3 |  |
| WT-17221 | Bulk insert flush — scale test: large table load and O(1) memory verification | Open | Task | Major - P3 |  |
| WT-17222 | Bulk insert flush — performance benchmarking: throughput and flush overhead | Open | Task | Major - P3 |  |
| WT-17231 | task-timed-out: format-abort-recovery-stress-test on ubuntu2004-stress-nonstandalone [wiredtiger @ ff005a58] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-17234 | Eviction queue scale feature: investigation log across 6 approaches | Open | Task | Major - P3 |  |
| WT-17236 | Per-btree dirty-index ring with adaptive drain throttling for eviction | Open | Improvement | Major - P3 |  |
| WT-17251 | Add logging to test format predictable replay | Backlog | Task | Major - P3 |  |
| WT-17254 | Make unpositioned cursors perform blind deletes when overwrite=true | Open | Task | Major - P3 | expedite |
| WT-17260 | Support ops on newly inserted keys in test format predictable replay | Backlog | Improvement | Major - P3 |  |
| WT-17264 | Investigate and improve error handling for __conn_config_file | Open | Task | Major - P3 | expedite |
| WT-17274 | Write prepared fast truncate to disk when preserve prepared config is enabled | Open | Task | Major - P3 |  |
| WT-17275 | Support prepare claim for fast truncate to reconstruct prepared transactions on restart | Open | Task | Major - P3 |  |
| WT-17276 | Rollback to stable: rollback prepared fast truncate | Open | Task | Major - P3 |  |
| WT-17277 | Add testing support for prepared fast truncate in test_checkpoint and test_format | Open | Task | Major - P3 |  |
| WT-17280 | Observe checkpoint perf in the fleet | Open | Epic | Major - P3 |  |
| WT-17281 | Long running checkpoints | Open | Story | Major - P3 |  |
| WT-17282 | Checkpoint cleanup performance | Open | Story | Major - P3 |  |
| WT-17283 | Success rate of checkpoints | Open | Story | Major - P3 |  |
| WT-17284 | Create a dashboard to capture successful/failed checkpoints | Backlog | Sub-task | Major - P3 |  |
| WT-17285 | Create p50, p75, p99 for checkpoint duration | Open | Sub-task | Major - P3 |  |
| WT-17290 | failed: format-stress-test-3 on ubuntu2004-stress-nonstandalone [wiredtiger @ f483bacd] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-17292 | failed: format-stress-test-no-barrier on ubuntu2004-stress-tests [wiredtiger-mongo-v8.0 @ 3f6eb6d9] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-17294 | Enable pre-positioning the cursor in test format predictable replay | Backlog | Bug | Major - P3 |  |
| WT-17299 | Cache metadata cursors internally | Backlog | Improvement | Major - P3 |  |
| WT-17308 | Dead local_stop path in __wt_session_range_truncate | Backlog | Bug | Major - P3 |  |
| WT-17311 | Modify that sees an outdated tombstone returnd WT_NOTFOUND instead of WT_ROLLBACK | Backlog | Bug | Major - P3 |  |
| WT-17313 | Server Victim (Block) Cache Improvements | Backlog | Epic | Minor - P4 |  |
| WT-17314 | failed: cppsuite-bounded-cursor-stress-stress on amazon2023-stress-tests-arm64 [wiredtiger @ 9028e94a] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-17317 | failed: precise-checkpoint-stress-test-tiered on ubuntu2004-stress-tests-arm64 [wiredtiger @ da3e42bb] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-17326 | Use of uninitialized memory on 8.0 | Open | Bug | Major - P3 |  |
| WT-17329 | Move stat.py-generated files out of git and into the build system | Backlog | Task | Major - P3 |  |
| WT-17335 | Document parallel checkpoints in the WT architecture guide | Open | Documentation | Major - P3 |  |
| WT-17348 | Generalise verify read_corrupt config to all modes in wt util | Open | Sub-task | Major - P3 |  |
| WT-17353 | Fix transaltion of eviction state in t2 | Open | Bug | Minor - P4 |  |
| WT-17364 | Investigate extending checkpoint failure panic to ASC | Open | Task | Major - P3 |  |
| WT-17366 | failed: format-stress-test-1 on ubuntu2004-release-stress-tests [wiredtiger @ 5beaca83] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-17368 | failed: format-stress-test-3 on amazon2023-stress-tests-arm64 [wiredtiger @ d7032a0b] | Open | Build Failure | Major - P3 | BB-Tools |
| WT-17371 | task-timed-out: recovery-stress-test-2 on ubuntu2004-arm64-nonstandalone [wiredtiger @ 3092d121] | Backlog | Build Failure | Major - P3 | BB-Tools |
| WT-17377 | Enforce that a prepared transaction's durable timestamp is greater than its prepare timestamp | Open | Task | Major - P3 |  |
| WT-17381 | TSAN data race in __wt_delete_page_rollback writing to instantiated tombstone concurrently read by cursor | Open | Bug | Major - P3 |  |
| WT-17384 | Add atomic variable for checking if the truncate list is empty | Open | Improvement | Major - P3 |  |
| WT-17389 | Two-phase eviction: performance investigation and regression analysis | Open | Task | Major - P3 |  |
| WT-17390 | Replace deprecated WT_ACQUIRE_READ_WITH_BARRIER/WT_RELEASE_WRITE_WITH_BARRIER macros in test/format with atomic operations | Open | Task | Major - P3 | neweng, quickwin |
| WT-17398 | Remove cloud storage source extensions (azure_store, gcp_store, s3_store) | Open | Epic | Major - P3 |  |
| WT-17399 | Remove s3_store extension | Open | Task | Major - P3 |  |
| WT-17400 | Remove azure_store extension | Open | Task | Major - P3 |  |
| WT-17401 | Remove gcp_store extension | Open | Task | Major - P3 |  |
| WT-17402 | Documentation cleanup after cloud storage source removal | Open | Task | Major - P3 |  |
| WT-17403 | Fix memory leak in fast truncate logging | Open | Improvement | Major - P3 |  |
