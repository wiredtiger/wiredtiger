# schema_disagg_abort

Stress test for disaggregated schema operations under crashes and role switches. One or two nodes
continuously create, populate and drop tables while sharing a PALite page log. The parent switches
roles, kills nodes or stops them cleanly, then recovers each node from shared storage and verifies
the durable state against per-operation record files.

## Architecture

- A leader generates events into a self-pipe. Its reader demultiplexes them into per-worker queues,
  and the workers apply and relay the events to the follower through a process pipe.
- A peered follower consumes the leader's relay. A lone follower generates its own workload but
  cannot make it durable until it becomes leader.
- In epoch mode, creates and drops have separate operation and publish events, allowing checkpoints
  and crashes to land between the two. In legacy mode each schema operation is complete without a
  publish event. Inserts are generated only after the create is complete.
- A published drop parks the slot in REMOVED until the stable schema epoch passes the drop's epoch;
  recreating the name earlier is an API violation WiredTiger panics on.
- Leaders write checkpoints to the shared metadata; followers pick them up.
- Role changes are events in the event stream, so all earlier work is drained before the connection
  is reconfigured.
- The generator captures pending publishes in a batch at step-down without changing table states.
  The marker carries the exact number of timestamps to reserve at or below the step-down timestamp.
  Each publish event carries its timestamp source: the step-down range for a batch member, or the
  current counter for a new operation. Workers apply both through the same publish path.
- The step-down workload interleaves batch members with new operations, which allocate above the
  boundary. Stable reaches the boundary only once all batch members have applied. Drops of published
  tables wait for the step-down checkpoint: a blocked drop could otherwise prevent a batch member
  queued behind it from applying.

## Directory layout

```text
WT_TEST.foo/
├── records/                      verifier ground truth
│   ├── node<N>-leader-<T>        operations originated by node N
│   └── node<N>-follower-<T>      peer operations applied by node N
├── PALITE/                       shared page log
├── WT_NODE0/                     node 0 local home
├── WT_NODE1/                     node 1 local home (`-r lf` only)
├── leader_ready                  ┐
├── follower_ready                │ sentinel files: the parent⇄node
├── switch_request                │ coordination protocol
├── switch_done.<k>               │
├── stop_run                      ┘
└── ckpt_adopted                  latest follower checkpoint LSN and schema epoch
WT_TEST.foo.SAVE/                 pre-verification copy of the home
```

## Generator state machine

Each worker owns a pool of table slots. The generator chooses a slot and takes one valid transition
or lingers, widening the window in which a checkpoint or crash can occur. (The diagram shows epoch
mode only.) States describe the generated event stream; workers report applied epochs separately.

```mermaid
stateDiagram-v2
    direction TB

    state "NONE - slot free" as NONE
    state "CREATED - create publish pending" as CREATED
    state "PUBLISHED - create published" as PUBLISHED
    state "DROPPED - drop publish pending" as DROPPED
    state "REMOVED - drop published, coverage pending" as REMOVED

    [*] --> NONE
    NONE --> CREATED : create
    CREATED --> CREATED : linger
    CREATED --> PUBLISHED : publish create
    CREATED --> NONE : cancel with drop
    PUBLISHED --> PUBLISHED : insert or linger
    PUBLISHED --> DROPPED : drop, once the peer covers the create and not before the step-down checkpoint
    DROPPED --> DROPPED : linger
    DROPPED --> REMOVED : publish drop
    REMOVED --> REMOVED : await coverage
    REMOVED --> NONE : stable epoch covers the drop
```

  Publication scheduling is separate from this lifecycle. Each slot stores its publish timestamp
  source in the workload state, which also holds the count of captured publishes not yet emitted.
  A captured publish must be emitted exactly once: its next slot visit publishes instead of canceling
  or lingering. The generator flushes the remaining captured publishes when the step-down event budget
  is reached or the peer dies. Once the count reaches zero, no further scans are needed. Handover
  asserts that the count is zero before publishing any remaining new operations. The checkpoint still
  waits for application, not just emission.

## Threads

Threads are created for each leader or follower phase.

| Thread | Count | Responsibility |
|---|---:|---|
| generator | 1 for a leader or lone node | Advance the slot state machines and emit workload and role-transition events. |
| reader | 1 | Read the self-pipe or peer pipe, demultiplex events and drain work at transition markers. |
| worker | `-T N` | Apply events, relay leader events, append records and report completed timestamps. |
| timestamp | 1 | Advance oldest and stable timestamps, plus the stable schema epoch in epoch mode, to the completed frontier. |
| checkpoint | 1 | Take leader checkpoint, or pick up checkpoints as follower. |

Shutdown is ordered generator, reader, workers, checkpoint, timestamp. The last two remain available
while workers drain because a schema operation can be waiting for a checkpoint.

## Recovery and verification

The verifier discards each node's local data and rebuilds it from the shared page log. Record files
describe creates, drops and inserts; records newer than the recovered durable schema epoch are
ignored. The verifier checks table presence, inserted values and the relayed event prefix.

Note: Legacy (epoch-less) mode has limited verification. Slots with schema operations after the last
checkpont cannot be verified reliably.

## Running

```text
test_schema_disagg_abort [-b build-dir] [-e] [-h dir] [-k [l|f]N] [-p] [-r l|f|lf]
                         [-s N] [-T threads] [-t time] [-u pool] [-v]
```

`-r` selects a lone leader, lone follower or leader/follower pair. `-s` schedules role switches,
`-k` schedules kills, and `-t` sets the graceful stop time; a run ends early once every node has
been killed. Every run prints a reproducible `CONFIG:` line including its random seeds. `-e` runs
legacy schema operations and requires a single-node `-r l` or `-r f` topology.
