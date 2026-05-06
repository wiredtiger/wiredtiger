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

**Distribution:**

| Team | Tickets | % |
|------|---------|---|
| Storage Engines - Foundations | 360 | 62.7% |
| Storage Engines - Transactions | 129 | 22.5% |
| Storage Engines - Persistence | 85 | 14.8% |
| Unclear | 0 | — |
| **Total** | **574** | |

No tickets were left as "Unclear" — all 574 had enough information for a confident assignment.

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
