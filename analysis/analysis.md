# WiredTiger Jira Triage: Missing "Assigned Teams" Field

## Goal

Identify and assign the "Assigned Teams" field on all open WiredTiger Jira issues
where it is currently empty, so each ticket is owned by the right sub-team.

## Steps

### Step 1 — Collect all issues without "Assigned Teams" (done 2026-04-30)

JQL filter used:
```
project = WiredTiger and status != Closed and "Assigned Teams" IS EMPTY ORDER BY key DESC
```

Result: **574 issues** (WT-999 through WT-12294).

All ticket links are in [issues.md](issues.md).

---

### Step 2 — Collect full ticket data (done 2026-04-30)

Read team charters for all three sub-teams:
- [Foundations Team Charter](team_charters/Team%20Charter_%20Foundations%20Team%20(1).md)
- [Persistence Team Charter](team_charters/Team%20Charter_%20Persistence%20Team%20(1).md)
- [Transactions Team Charter](team_charters/Team%20Charter_%20Transactions%20Team%20(1).md)

Fetched full details (summary, status, type, priority, labels, components, assignee, reporter, created, updated, description) for all 574 tickets using `mcp__MYJIRA__get_issue`. Data written to 6 files split by key range (~96 tickets each):

| File | Key range | Tickets |
|------|-----------|---------|
| [tickets_data_1.md](tickets_data_1.md) | WT-999 – WT-6076 | 96 |
| [tickets_data_2.md](tickets_data_2.md) | WT-6100 – WT-7495 | 96 |
| [tickets_data_3.md](tickets_data_3.md) | WT-7503 – WT-8810 | 96 |
| [tickets_data_4.md](tickets_data_4.md) | WT-8811 – WT-9808 | 96 |
| [tickets_data_5.md](tickets_data_5.md) | WT-9810 – WT-10855 | 96 |
| [tickets_data_6.md](tickets_data_6.md) | WT-10865 – WT-12294 | 94 |

---

### Step 3 — Team assignment analysis (done 2026-04-30)

For every ticket, read its full data and reasoned against the three team charters to assign ownership. Results written to [assignments.md](assignments.md).

**Initial distribution (superseded by Step 6 — see below for current numbers):**

| Team | Tickets | % |
|------|---------|---|
| Storage Engines - Foundations | 360 | 62.7% |
| Storage Engines - Transactions | 129 | 22.5% |
| Storage Engines - Persistence | 85 | 14.8% |
| Unclear | 0 | — |
| **Total** | **574** | |

No tickets were left as "Unclear" — all 574 had enough information for a confident assignment.

**Current distribution (after Step 6 deprecated-feature reassignment):**

| Team | Tickets | % |
|------|---------|---|
| Storage Engines - Foundations | 350 | 60.9% |
| Storage Engines - Transactions | 128 | 22.3% |
| Storage Engines - Persistence | 96 | 16.7% |
| Unclear | 0 | — |
| **Total** | **574** | |

---

---

### Step 4 — Urgency / priority flags (done 2026-05-06)

Each ticket was reviewed for urgency signals: data integrity/corruption, security issues, crashes, hangs, customer-reported incidents, recent activity, and incorrect MVCC/timestamp behavior. Results written to [urgency_flags.md](urgency_flags.md).

**Summary:**

| Category | Tickets |
|----------|---------|
| High Urgency (corruption, security, crash, hang) | 22 |
| Medium Urgency (recently renewed, perf pathologies, coverage gaps) | 13 |
| **Total flagged** | **35** |

Top items:
- **WT-10829** — Security: cloud storage credentials (Azure/AWS) printed in logs on config error (labeled `security`, updated 2025-12)
- **WT-3965** — Schema operations not atomic — root cause of crash-on-restart bugs; active as of late 2025
- **WT-8278** — Salvage introduces new HS corruption; recently active (2025-03)
- **WT-12010** — Active corruption event detected today (2026-05-06) in a Testy run
- **WT-11244** — MSAN: uninitialized bytes in bulk-load write path; recently active

---

### Step 5 — Won't Do / already done candidates (done 2026-05-06)

Each ticket was reviewed for signs that the work is obsolete, was completed under a different ticket, or describes technology that no longer exists. Results written to [wont_do_candidates.md](wont_do_candidates.md).

**Summary:**

| Category | Tickets |
|----------|---------|
| Strong candidates (high confidence, can close now) | 19 |
| Moderate candidates (verify before closing) | 16 |
| **Total candidates** | **35** |

Top items:
- **WT-5035** — Decommission Jenkins CI (Jenkins is long gone)
- **WT-8082–8739** (11 tickets) — Auto-generated boilerplate "Architecture Guide update" sub-tasks for past PM milestones
- **WT-6977 / WT-7017** — Write-up about C++ conversion (conversion is complete, write-up never happened, project has moved on)
- **WT-3723** — Timestamp support for wtperf (description says use workgen; workgen is now the standard)

---

### Step 6 — Deprecated-feature reassignment: Tiered Storage & Column Store (done 2026-05-06)

Both tiered storage and column store are deprecated. All 25 tickets primarily about these features were assigned to **Storage Engines - Persistence** and added to [wont_do_candidates.md](wont_do_candidates.md) as close or low-priority candidates.

**Assignment changes:**

| Ticket | Old Team | New Team |
|--------|----------|----------|
| WT-3626 | Transactions | Persistence |
| WT-7518 | Foundations | Persistence |
| WT-7734 | Foundations | Persistence |
| WT-8916 | Foundations | Persistence |
| WT-8977 | Foundations | Persistence |
| WT-9658 | Foundations | Persistence |
| WT-9808 | Foundations | Persistence |
| WT-10794 | Foundations | Persistence |
| WT-10829 | Foundations | Persistence |
| WT-10936 | Foundations | Persistence |
| WT-10991 | Foundations | Persistence |

14 tickets were already assigned to Persistence and remain unchanged.

**Updated team distribution:** Foundations 350 (60.9%), Transactions 128 (22.3%), Persistence 96 (16.7%).

---

### Step 7 — Architecture guide and documentation update sub-task reassignment (done 2026-05-06)

All auto-generated architecture guide update (WT-8082–WT-8739) and documentation update (WT-9460–WT-9574) sub-tasks were previously assigned to Foundations as boilerplate. Each references a parent project ticket. The referenced "PM-XXXX" prefixes are actually **SPM-XXXX** in Jira. Each SPM ticket was fetched and the WT sub-task was assigned to the team that owns the parent project's subject matter.

**Assignment changes:**

| Ticket | SPM | SPM Subject | Old Team | New Team |
|--------|-----|------------|----------|----------|
| WT-8082 | SPM-2503 | Export/Import | Foundations | Persistence |
| WT-8083 | SPM-2504 | History Store | Foundations | Transactions |
| WT-8084 | SPM-2505 | History Store | Foundations | Transactions |
| WT-8085 | SPM-2506 | History Store | Foundations | Transactions |
| WT-8087 | SPM-2507 | Salvage | Foundations | Persistence |
| WT-8088 | SPM-2508 | Checkpoint | Foundations | Persistence |
| WT-8089 | SPM-2509 | History Store | Foundations | Transactions |
| WT-8090 | SPM-2510 | History Store | Foundations | Transactions |
| WT-8215 | SPM-2564 | Timestamp Interface | Foundations | Transactions |
| WT-9532 | SPM-2961 | Shard Merge Timestamps (Canceled) | Foundations | Transactions |
| WT-9574 | SPM-2975 | Cache Observability | Foundations | Transactions |

**Tickets remaining at Foundations (SPM subject is Foundations work):**

| Ticket | SPM | SPM Subject | SPM Status |
|--------|-----|------------|------------|
| WT-8334 | SPM-2631 | Logging/Metrics | Done |
| WT-8738 | SPM-2710 | Test Framework | Done |
| WT-8739 | SPM-2711 | Test Framework | Done |
| WT-9460 | SPM-2942 | API/Session Management | Done |
| WT-9461 | SPM-2943 | API/Session Management | Done |
| WT-9464 | SPM-2944 | Count/Size API (Canceled) | Canceled |
| WT-9531 | SPM-2960 | gRPC/Networking (Canceled) | Canceled |

Notes on Canceled parents: WT-9464 (SPM-2944), WT-9531 (SPM-2960), and WT-9532 (SPM-2961) all have Canceled parent SPM projects. These are **strong Won't Do candidates** — the parent features were never shipped so no documentation is needed.

The reason lines in [wont_do_candidates.md](wont_do_candidates.md) and [assignments.md](assignments.md) were updated for all 18 tickets with SPM-specific details.

**Final team distribution (after Steps 6 + 7):** Foundations 339 (59.1%), Transactions 136 (23.7%), Persistence 99 (17.2%).
