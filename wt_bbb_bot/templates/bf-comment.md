# BF Jira Comment Template

Use this when posting via `jira_add_comment`. Show the user the exact text and get
confirmation before posting.

---

```
h2. AI-Assisted Triage and Investigate

*Ticket:* WT/BF-XXXXX | *Age:* N days | *Type:* <type> | *Subsystem:* <name>

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

*Confidence:* Low / Medium / High — <key uncertainty in one clause>

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
