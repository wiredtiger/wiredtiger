# WiredTiger Won't Do / Already Done Candidates

Analysis of 574 open WiredTiger tickets with missing "Assigned Teams" fields.
Date: 2026-05-06

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

### WT-8082 — Architecture Guide update for PM-2503
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Boilerplate ticket: "Investigate if this project requires changes to the architecture guide." These sub-tasks were created automatically for every project milestone. PM-2503's work is long past; any architecture guide updates that were needed have either been done or are no longer relevant.

---

### WT-8083 — Architecture Guide update for PM-2504
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same as WT-8082 — boilerplate architecture guide investigation sub-task for a PM milestone that has long since closed.

---

### WT-8084 — Architecture Guide update for PM-2505
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2505 milestone is past.

---

### WT-8085 — Architecture Guide update for PM-2506
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2506 milestone is past.

---

### WT-8087 — Architecture Guide update for PM-2507
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2507 milestone is past.

---

### WT-8088 — Architecture Guide update for PM-2508
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2508 milestone is past.

---

### WT-8089 — Architecture Guide update for PM-2509
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2509 milestone is past.

---

### WT-8090 — Architecture Guide update for PM-2510
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2510 milestone is past.

---

### WT-8215 — Architecture Guide update for PM-2564
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2564 milestone is past.

---

### WT-8334 — Architecture Guide update for PM-2631
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2631 milestone is past.

---

### WT-8738 — Architecture Guide update for PM-2710
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2710 milestone is past.

---

### WT-8739 — Architecture Guide update for PM-2711
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same boilerplate sub-task. PM-2711 milestone is past.

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

### WT-9460 — Documentation update sub-task (PM-2942)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Boilerplate "update all types of documentation" sub-task for a PM milestone. These were auto-generated; if the parent PM feature work was completed and shipped, any required documentation was either written or deliberately skipped. Verify whether the parent feature (PM-2942) ever had documentation written.

---

### WT-9461 — Documentation update sub-task (PM-2943)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same as WT-9460 — boilerplate doc update sub-task for PM-2943.

---

### WT-9464 — Documentation update sub-task (PM-2944)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same as WT-9460 — boilerplate doc update sub-task for PM-2944.

---

### WT-9531 — Documentation update sub-task (PM-2960)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same as WT-9460 — boilerplate doc update sub-task for PM-2960.

---

### WT-9532 — Documentation update sub-task (PM-2961)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same as WT-9460 — boilerplate doc update sub-task for PM-2961.

---

### WT-9574 — Documentation update sub-task (PM-2975)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Reason:** Same as WT-9460 — boilerplate doc update sub-task for PM-2975.

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
