# WiredTiger Won't Do / Already Done Candidates

Analysis of 574 open WiredTiger tickets with missing "Assigned Teams" fields.
Date: 2026-05-06

---

## DEPRECATED FEATURE TICKETS: Tiered Storage & Column Store

Both **tiered storage** and **column store** are now deprecated. All tickets primarily about these features are assigned to **Storage Engines - Persistence** and are candidates for **lower priority or closure**. The table below lists all 25 affected tickets.

| Ticket | Title | Feature | Confidence | Rationale |
|--------|-------|---------|-----------|-----------|
| WT-3626 | Allow updates to be restored against an empty column store page | Column Store | Close | Core column store eviction bug — feature deprecated |
| WT-7518 | Update WT_DATA_HANDLE to support different types of backing storage for Btrees | Tiered Storage | Close | Tiered storage dhandle infrastructure — feature deprecated |
| WT-7693 | Fix tiered storage disconnect between WT_BUCKET_STORAGE and customize_file_system | Tiered Storage | Close | Tiered storage filesystem API bug — feature deprecated |
| WT-7734 | Add dhandle flag to indicate dhandles that are both btree and object | Tiered Storage | Close | Tiered storage dhandle flag — feature deprecated |
| WT-7735 | Support tiered tables in wt_block_checkpoint_last | Tiered Storage | Close | Tiered table checkpoint support — feature deprecated |
| WT-7927 | incr_backup test doesn't test variable- or fixed-length column store access methods | Column Store | Close | Incremental backup test for column store — feature deprecated |
| WT-8445 | Add VLCS/FLCS cases for test_checkpoint/recovery-test.sh | Column Store | Close | Column store (VLCS/FLCS) recovery tests — feature deprecated |
| WT-8763 | Logging and extension API improvements for storage sources | Tiered Storage | Low-pri | Tiered storage extension logging; the logging API may have broader use |
| WT-8916 | Enable S3 extension build and test on the Windows | Tiered Storage | Close | S3 extension Windows build — feature deprecated |
| WT-8977 | Tiered Storage python tests shouldn't check contents of dir_store cache | Tiered Storage | Close | Tiered storage test cleanup — feature deprecated |
| WT-9145 | Add donor_stable_timestamp in WT_SESSION::create(import=()) | Tiered Storage | Low-pri | Import+RTS functionality; the import path itself is still live but tiered context reduces urgency |
| WT-9658 | Add visible statistics for s3_store module | Tiered Storage | Close | S3 statistics — feature deprecated |
| WT-9808 | Fix suite_subprocess.runWt for tiered storage | Tiered Storage | Close | Test infrastructure for tiered storage — feature deprecated |
| WT-10182 | Add configuration to s3_store to turn off file caching | Tiered Storage | Close | S3 store configuration — feature deprecated |
| WT-10794 | Save WT files in cloud storage as part of test artifacts | Tiered Storage | Close | CI artifact collection for tiered tests — feature deprecated |
| WT-10829 | Redact AccountKey when printing out configuration passed into WiredTiger | Tiered Storage | Low-pri | Security bug (credentials in logs); still relevant until tiered code is removed, but lower urgency now that the feature is deprecated |
| WT-10850 | s3 subsystem not printing error messages by default | Tiered Storage | Close | S3 extension logging bug — feature deprecated |
| WT-10936 | Make test/checkpoint predictable for column store | Column Store | Close | Column store test/checkpoint fix — feature deprecated |
| WT-10991 | Add "general" handler callbacks to Python SWIG interface | Tiered Storage | Close | SWIG interface callbacks for tiered storage — feature deprecated |
| WT-11004 | Prevent tiered objects from being overwritten in s3, gcp, azure | Tiered Storage | Close | Write-once semantics for cloud objects — feature deprecated |
| WT-11185 | Prototype tiered storage compaction | Tiered Storage | Close | Tiered storage compaction prototype — feature deprecated |
| WT-11375 | Allow the S3 extension to use AWS sso | Tiered Storage | Close | S3 authentication improvement — feature deprecated |
| WT-11376 | Allow the Azure extension to use Azure AD | Tiered Storage | Close | Azure authentication improvement — feature deprecated |
| WT-11377 | Allow the GCP extension to use Application Default Credentials (ADC) | Tiered Storage | Close | GCP authentication improvement — feature deprecated |
| WT-11404 | Do not create tiered table's local file until first write | Tiered Storage | Close | Tiered table local file optimization — feature deprecated |

**Legend:** *Close* = can be closed now as Won't Do. *Low-pri* = retain but deprioritize; worth a quick human check before closing.

> **Cross-reference:** WT-10829 also appears in [urgency_flags.md](urgency_flags.md) as a security/high-urgency ticket. Now that tiered storage is deprecated, its urgency is reduced — but the fix is still advisable before the code is removed.

---

---

## STRONG CANDIDATES

Tickets very likely obsolete, already completed, or explicitly describing work that no longer applies.

---

### WT-5035 — Decommission Jenkins CI system
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Jenkins CI was decommissioned years ago. The ticket has no description, was created in 2019, and the work it describes is verifiably complete — WiredTiger uses Evergreen CI. This ticket should be closed as Done or Won't Do.

---

### WT-10130 — Review wtperf_run.sh scripts for removal
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** The ticket description explicitly states the scripts "were previously used in Jenkins but no longer seem to be used." Jenkins is decommissioned; the scripts are already unused. Close as Won't Do or Done.

---

### WT-3723 — Add timestamp support to wtperf
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Foundations
- **Reason:** The description explicitly says the "long term plan is to use workgen" instead of wtperf for this kind of testing. Workgen is now the active benchmark framework. Adding timestamp support to wtperf is no longer on the roadmap.

---

### WT-6977 — Write about "Converting WiredTiger into C++ project"
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** WiredTiger has already been converted to a C++ project. This was a retrospective action item from 2020-2021 to document the conversion. The conversion is done; this documentation ticket is stale.

---

### WT-7017 — Write-up: Converting WiredTiger to C++ (follow-up)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same as WT-6977 — WiredTiger is already a C++ project. This is a companion documentation ticket from the same 2020-2021 retrospective. The conversion is complete; the document was never written but the project has moved on.

---

### WT-8031 — Fix many-dhandles-stress.py for range partition
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Reason:** The description explicitly states "WT-7332 is complete and closed" as a prerequisite for this fix being meaningful. WT-7332 is indeed closed. However, this fix was never applied and the ticket has aged significantly. The test it fixes may no longer reflect the current code or range partition implementation. Candidate for Won't Do if the test still works acceptably.

---

### WT-10244 — Unresolved issue in many-dhandle-stress.py
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Reason:** References WT-7332 as "not yet fixed" as the blocker. WT-7332 is now fixed and closed. The original reason this stress test issue was deferred is resolved, but this ticket has not been revisited in years. If the stress test now passes, this can be closed as Done.

---

### WT-8082 — Architecture Guide update for SPM-2503 (Export/Import)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Persistence
- **Reason:** SPM-2503 covers the export/import feature (file-level export and import of WiredTiger tables). Export/import is a Persistence concern (block manager, file management, checkpoint integration). SPM-2503 is Done. Boilerplate architecture guide sub-task; the feature shipped and any needed documentation was either written or skipped. Safe to close as Won't Do.

---

### WT-8083 — Architecture Guide update for SPM-2504 (History Store)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Transactions
- **Reason:** SPM-2504 covers History Store improvements. The history store is owned by Transactions. SPM-2504 is Done. Boilerplate architecture guide sub-task for a completed Transactions-owned milestone. Safe to close as Won't Do.

---

### WT-8084 — Architecture Guide update for SPM-2505 (History Store)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Transactions
- **Reason:** SPM-2505 covers History Store improvements. The history store is owned by Transactions. SPM-2505 is Done. Boilerplate architecture guide sub-task for a completed Transactions-owned milestone. Safe to close as Won't Do.

---

### WT-8085 — Architecture Guide update for SPM-2506 (History Store)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Transactions
- **Reason:** SPM-2506 covers History Store improvements. The history store is owned by Transactions. SPM-2506 is Done. Boilerplate architecture guide sub-task for a completed Transactions-owned milestone. Safe to close as Won't Do.

---

### WT-8087 — Architecture Guide update for SPM-2507 (Salvage)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Persistence
- **Reason:** SPM-2507 covers Salvage (corruption recovery) improvements. Salvage is owned by Persistence. SPM-2507 is Done. Boilerplate architecture guide sub-task for a completed Persistence-owned milestone. Safe to close as Won't Do.

---

### WT-8088 — Architecture Guide update for SPM-2508 (Checkpoint)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Persistence
- **Reason:** SPM-2508 covers Checkpoint improvements. Checkpoints are owned by Persistence. SPM-2508 is Done. Boilerplate architecture guide sub-task for a completed Persistence-owned milestone. Safe to close as Won't Do.

---

### WT-8089 — Architecture Guide update for SPM-2509 (History Store)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Transactions
- **Reason:** SPM-2509 covers History Store improvements. The history store is owned by Transactions. SPM-2509 is Done. Boilerplate architecture guide sub-task for a completed Transactions-owned milestone. Safe to close as Won't Do.

---

### WT-8090 — Architecture Guide update for SPM-2510 (History Store)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Transactions
- **Reason:** SPM-2510 covers History Store improvements. The history store is owned by Transactions. SPM-2510 is Done. Boilerplate architecture guide sub-task for a completed Transactions-owned milestone. Safe to close as Won't Do.

---

### WT-8215 — Architecture Guide update for SPM-2564 (Timestamp Interface)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Transactions
- **Reason:** SPM-2564 covers Timestamp Interface improvements (the public API for setting/querying timestamps). Timestamp semantics and MVCC are owned by Transactions. SPM-2564 is Done. Boilerplate architecture guide sub-task for a completed Transactions-owned milestone. Safe to close as Won't Do.

---

### WT-8334 — Architecture Guide update for SPM-2631 (Logging/Metrics)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** SPM-2631 covers logging infrastructure and metrics improvements. Logging (WAL), metrics, and observability infrastructure are owned by Foundations. SPM-2631 is Done. Boilerplate architecture guide sub-task for a completed Foundations-owned milestone. Safe to close as Won't Do.

---

### WT-8738 — Architecture Guide update for SPM-2710 (Test Framework)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** SPM-2710 covers test framework and CI tooling improvements. Test infrastructure and CI tooling are owned by Foundations. SPM-2710 is Done. Boilerplate architecture guide sub-task for a completed Foundations-owned milestone. Safe to close as Won't Do.

---

### WT-8739 — Architecture Guide update for SPM-2711 (Test Framework)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** SPM-2711 covers test framework and CI tooling improvements. Test infrastructure and CI tooling are owned by Foundations. SPM-2711 is Done. Boilerplate architecture guide sub-task for a completed Foundations-owned milestone. Safe to close as Won't Do.

---

---

## MODERATE CANDIDATES

Tickets that are likely obsolete or superseded but require a quick verification before closing.

---

### WT-4938 — Error installing WiredTiger Python module on Windows (pip install)
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Foundations
- **Reason:** Created in 2019, refers to WiredTiger v3.1.0 PyPi package installation failure on Windows. The PyPi package hasn't been actively maintained for years and the WiredTiger project no longer distributes standalone Python packages via PyPi as a primary channel. Likely obsolete technology. Verify PyPi packaging status before closing.

---

### WT-6795 — Remove random_directio debugging
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Persistence
- **Reason:** The description says to remove this debug code "when the problem is solved." The problem it was debugging was filed years ago. Verify whether the debug code still exists in the codebase; if removed, close as Done.

---

### WT-6699 — Create Evergreen task for modified LSWA workload
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** References DSI (Database Storage Integration) workload infrastructure. DSI and the LSWA workload toolchain may no longer be used or may have been superseded by newer performance testing frameworks. Verify if LSWA is still actively used.

---

### WT-7503 — Change default compressor for WT HS to Zstandard
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Transactions
- **Reason:** Zstandard (zstd) is now widely available and is used throughout the codebase. There is a reasonable probability that the history store default compressor was already changed as part of broader zstd adoption work. Verify current default HS compressor configuration before deciding.

---

### WT-7527 — Fine-tuning reverse modifies for HS records
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Transactions
- **Reason:** References `WT_MAX_CONSECUTIVE_REVERSE_MODIFY` constant and fine-tuning the threshold. The history store reconciliation code has had significant work since this ticket was filed (2021). The specific tuning described may have been superseded by architectural changes to how HS records are managed.

---

### WT-7576 — Remove --zstd option once zstd installed on PPC/ZSeries
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** The ticket was created as a contingency — "remove this flag once zstd is available on PPC and ZSeries." If zstd has been installed on those platforms (which is likely given the time elapsed since 2021), the flag removal is overdue. Verify zstd availability on PPC/ZSeries test platforms.

---

### WT-9460 — Documentation update sub-task (SPM-2942)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** SPM-2942 covers API and session management improvements (Foundations-owned work). Auto-generated documentation sub-task. SPM-2942 is Done. Any required documentation was either written or skipped when the feature shipped. Safe to close as Won't Do, or verify first whether SPM-2942 documentation was ever produced.

---

### WT-9461 — Documentation update sub-task (SPM-2943)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** SPM-2943 covers API and session management improvements (Foundations-owned work). Auto-generated documentation sub-task. SPM-2943 is Done. Safe to close as Won't Do.

---

### WT-9464 — Documentation update sub-task (SPM-2944)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** SPM-2944 covers count/size estimation API work (Storage Execution team, now Canceled). The WT-side documentation work would have been Foundations-adjacent (API layer). SPM-2944 was Canceled — the parent project never shipped. Strong Won't Do: the feature was canceled so no documentation is needed.

---

### WT-9531 — Documentation update sub-task (SPM-2960)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** SPM-2960 covers gRPC/networking work (Networking & Observability team, Canceled). Not a WiredTiger-internal feature; WiredTiger documentation here would be Foundations API layer at most. SPM-2960 was Canceled. Strong Won't Do: the feature was canceled so no documentation is needed.

---

### WT-9532 — Documentation update sub-task (SPM-2961)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Transactions
- **Reason:** SPM-2961 covers shard merge timestamp support (Service Architecture team, Canceled). Timestamp/MVCC documentation would be Transactions-owned work. SPM-2961 was Canceled. Strong Won't Do: the feature was canceled so no documentation is needed. Team reassigned from Foundations to Transactions based on SPM ownership.

---

### WT-9574 — Documentation update sub-task (SPM-2975)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Transactions
- **Reason:** SPM-2975 covers cache observability / eviction metrics improvements. Cache and eviction are owned by Transactions. SPM-2975 status and whether documentation was produced should be verified, but given the sub-task is auto-generated and the work is old, this is likely a Won't Do. Team reassigned from Foundations to Transactions based on SPM ownership.

---

### WT-10396 — Use stat cursor instead of separate API for record count
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Persistence
- **Reason:** References a specific design for using a stat cursor API for record count. Given the significant work on the stats infrastructure since 2022, this specific approach may have been superseded by a different design. Verify whether the current stats/record-count API matches the approach described.

---

### WT-11388 — Investigate volatility in overflow-130k Btree Throughput perf charts
- **Priority:** P4 | **Status:** Open | **Type:** Improvement
- **Team:** Foundations (wtperf component)
- **Reason:** References performance charts and the wtperf overflow-130k test configuration. The Atlas performance dashboard and the test infrastructure behind it may have changed significantly. Verify whether this specific test and its charts still exist in the current Evergreen configuration.

---

### WT-11502 — Migrate upload_stats_atlas.py to wiredtiger repo
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** The decision to migrate a script from `automation-script` repo to the wiredtiger repo. This was filed in 2023 with no updates. Either the migration was done (close as Done) or the script was removed/superseded. Verify current location of `upload_stats_atlas.py`.

---

### WT-11796 — Create a nice-looking README.md for WT on GitHub
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Foundations
- **Reason:** Filed in 2023 to improve the GitHub README. Check whether a `README.md` has since been created in the repository. If the file now exists and is reasonable, close as Done.

---

### WT-9615 through WT-9620 — Fail point implementation sub-tasks
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** A series of small fail point implementation sub-tasks spawned in 2022. The fail point framework has had considerable work done since then. Individual sub-tasks in a completed initiative may have been done implicitly. Verify which (if any) of these specific fail points were actually implemented.

---
