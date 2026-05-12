# Output Template

Keep everything tight. Bullet points over paragraphs. One line per fact.

When posting to Jira via `jira_add_comment`, use the Jira wiki markup version at the bottom of this file. Show the user the exact text and get confirmation before posting.

---

## AI-Assisted Triage and Investigate

**Ticket:** WT-XXXXX — `<summary>`
**Status:** `<status>` | **Age:** `<N days>` | **Type:** `<Crash / Assertion / Hang / Corruption / Flaky / Infra>` | **Subsystem:** `<name>`

---

## Priority Assessment

- `<One sentence: what kind of failure is this and how bad>`
- `<One sentence: how broad / who is affected>`
- `<One sentence: production impact or lack thereof>`

**Recommended priority:** P1 / P2 / P3 / P4

---

## Root Cause

**TL;DR:** `<One sentence — the core problem a reader can grasp in 5 seconds>`

- **What:** `<test/component that failed>`
- **Where:** `<file:function>`
- **Why:** `<two to four sentences — exact mechanism, function names, data flow>`

**Confidence:** Low / Medium / High / Very High — `<key uncertainty in one clause>`

---

## Recommended Fix  ← root cause known AND fix is clear

`<One to two sentences — specific change, file, function. Cite existing patch if present.>`

**Story points:** `<1 / 2 / 3 / 5 / 8 / 13 / 21>` | **Regression risk:** Low / Medium / High — `<one clause>`

---

## Fix Options  ← root cause known BUT fix is unclear

**Option 1 (preferred):** `<approach>`
Story points: `<N>` | Regression risk: `<level>` | Trade-off: `<one clause>`

**Option 2:** `<approach>`
Story points: `<N>` | Regression risk: `<level>` | Trade-off: `<one clause>`

**Option 3 (if applicable):** `<approach>`
Story points: `<N>` | Regression risk: `<level>` | Trade-off: `<one clause>`

---

## Next Action  ← root cause unclear

- Investigate deeper → @paths/investigate.md (`<specific question>`)
- Reproduce locally → @paths/build.md (`<repro command>`)
- Already fixed by `<commit/ticket>` — close as Won't Fix
- Close as infra (`<evidence>`)

---

> AI-assisted. Review before acting.

---

# Jira Comment Format

Use this block verbatim when calling `jira_add_comment`.

```
h2. AI-Assisted Triage and Investigate

*Ticket:* WT-XXXXX | *Age:* N days | *Type:* <type> | *Subsystem:* <name>

h3. Priority Assessment
* <One sentence: what kind of failure and how bad>
* <One sentence: how broad / who is affected>
* <One sentence: production impact or lack thereof>

*Recommended priority:* P1 / P2 / P3 / P4

h3. Root Cause
*TL;DR:* <One sentence — the core problem a reader can grasp in 5 seconds>

* *What:* <test/component that failed>
* *Where:* <file:function>
* *Why:* <two to four sentences — exact mechanism, function names, data flow>

*Confidence:* Low / Medium / High / Very High — <key uncertainty in one clause>

h3. Recommended Fix
<One to two sentences — specific change, file, function. Cite existing patch if present.>

*Story points:* N | *Regression risk:* Low / Medium / High — <one clause>

h3. Fix Options _(if fix unclear)_
*Option 1 (preferred):* <approach>
Story points: N | Regression risk: <level> | Trade-off: <one clause>

*Option 2:* <approach>
Story points: N | Regression risk: <level> | Trade-off: <one clause>

h3. Next Action _(if root cause unclear)_
<Single line>

----
_AI-assisted via wt-bbb-bot. Review before acting._
```
