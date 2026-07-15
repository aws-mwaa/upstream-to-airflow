<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# AIP-XXX: Enhanced Pools

> Working draft of an Airflow Improvement Proposal. The canonical copy, once a number is assigned,
> lives on the Airflow Confluence wiki and is discussed on the dev mailing list. Code is referenced
> by module, class, and function name rather than by line number, matching the style of accepted
> AIPs — the intent is to point at mechanisms, not exact positions.

## Status

| | |
|---|---|
| State | Draft |
| Discussion thread | To be created on dev@airflow.apache.org |
| Created | 2026-07 |
| Author(s) | Ramit Kataria |
| In release | Targets Airflow 3.x |
| Related | Builds on the mechanism from AIP-42 (Dynamic Task Mapping); Feature 3 overlaps AIP-100 (Eliminate Scheduler Queueing Starvation On Concurrency Limits) |

## Motivation

Pools are how Airflow limits concurrency for a set of tasks that share a scarce resource. A task
declares a pool and how many slots it takes, and the scheduler only lets the task run when that pool
has enough free slots. This works well, but the current design has three limitations that come up
often for teams running Airflow as a shared platform:

1. **A pool cannot be chosen at run time.** The pool and slot count are fixed in the Dag file. A
   pipeline that only knows which resource it needs after inspecting its input (a large versus small
   job, a GPU versus CPU model, a specific tenant) cannot express that. Setting a task's pool
   dynamically is asked for in [#33657](https://github.com/apache/airflow/issues/33657) (choosing the
   pool for a dynamically mapped task); resolving the slot count the same way is asked for in
   [#35803](https://github.com/apache/airflow/issues/35803).

2. **A task can only belong to one pool.** A task that consumes two scarce resources at once (say a
   database connection and a GPU) cannot be gated on both. Users work around this by creating one
   combined pool, which couples unrelated limits.

3. **Pools cannot reserve capacity for important work.** A pool has a single slot count shared by all
   tasks that use it. There is no way to keep a few slots aside so that high priority tasks can always
   run even when the pool is otherwise full. `priority_weight` only changes the order tasks are
   considered in, not reservation, so a burst of low priority tasks can still take every slot and hold
   it until those tasks finish.

The obvious idea for the first limitation, writing `pool="{{ ... }}"` and letting Jinja fill it in,
does not work as-is and needs care to make work. The scheduler decides which tasks to run inside a
locked critical section (`SchedulerJobRunner._executable_task_instances_to_queued`) that reads
`TaskInstance.pool` and `TaskInstance.pool_slots` straight from the database as concrete values,
looks the pool up by name, and decrements its free slots. The scheduler never runs user code and
never renders templates, by design, so a template string sitting in the pool column would be
treated as a pool that does not exist and the task would never be scheduled. Any dynamic behaviour
here therefore has to be declarative: the scheduler must be handed data it can read directly, not
code it has to execute. The template has to be resolved into a concrete value *before* the scheduler
reads it.

This AIP proposes three related enhancements to pools that stay within that constraint. They are
independent and can ship one at a time, in the order listed. Feature 3 overlaps with in-progress
scheduler work (AIP-100) and needs to be coordinated with it, as explained in its design.

### A note on terms

- **Slot**: one unit of a pool's capacity. A pool has a fixed number of slots.
- **`pool_slots`**: how many slots a task takes from its pool while it is running. The default is 1.
- **`priority_weight`**: a value the scheduler uses to order candidate tasks, highest first. It only
  affects ordering today, not how many slots a task is allowed to take. The value the scheduler
  orders by is the *effective* weight computed from the task's weight rule (absolute, downstream, or
  upstream), which can differ from the integer the author wrote — a point that matters for Feature 3.
- **Dynamic pool**: a pool name that is resolved at run time — from an upstream task's output or from
  a template — rather than being a fixed string in the Dag file. Introduced by Feature 1. The slot
  count stays fixed; resolving the slot count dynamically is discussed under Out of scope.
- **Reserved slots**: a portion of a pool's slots that stay available for the highest priority tasks,
  so that ordinary tasks cannot occupy the whole pool. Introduced by Feature 3.

## Proposal

### Feature 1: choose a task's pool at run time

Today a task's pool is fixed in the Dag file. This feature lets it be decided during the run. It
comes in two phases: resolving the pool from an upstream task's output, and resolving a templated
pool string without an explicit producer task.

#### Phase 1: resolve the pool from an upstream task's output

Let a task's `pool` be set to the output of an upstream task, the same way any other value can be
passed between tasks:

```python
from airflow.sdk import task

@task
def choose_pool(**context) -> str:
    return "gpu_pool" if needs_gpu(context) else "cpu_pool"

picked = choose_pool()

@task(pool=picked)
def run_model():
    ...
```

Setting `pool` to a task's output creates a dependency: `run_model` cannot be considered for
scheduling until `choose_pool` has finished and its return value is stored. No explicit `>>` is
needed — passing the task output wires the edge, just as passing it to any other argument would.
Writing a plain string, `pool="cpu_pool"`, keeps working exactly as before with no change to how it
is stored.

This phase supports a single upstream value that resolves to one pool name. The slot count stays a
fixed number. Wider forms are listed under Out of scope.

#### Phase 2: resolve a templated pool string

Often the pool does not depend on data computed during the run at all — it is a function of the
run's parameters or configuration, and the author would rather write a template than a whole task:

```python
@task(pool="{{ params.tier }}_pool")
def run_model():
    ...
```

Writing a separate task just to turn `params.tier` into a pool name is boilerplate this phase aims
to remove. The difficulty is the same constraint that runs through this whole AIP: the scheduler
never renders templates, so the string has to be resolved into a concrete pool name *before* the
scheduler reads it. Where that resolution happens is the open design question for this phase.
Several points are possible:

- **Render at task-instance creation** from run-static context (`params`, `dag_run.conf`, `ds`, and
  similar). This needs no upstream task and no XCom, because the values are known when the run
  starts. It only covers templates that do not depend on another task's output.
- **Auto-generate a small producer task** that renders the string and publishes the result, so
  Phase 1's machinery reads it back. This removes the manual task, but a template's dependency on an
  upstream XCom cannot in general be discovered from the template text, so the producer cannot always
  know what it must wait for — and framework-inserted task nodes are a new concept with their own UI
  and serialization questions.
- **Render on a worker or the triggerer**, both of which are allowed to run user code, and write the
  result back for the scheduler.

Phase 2 is deferred until Phase 1 has landed, and the resolution mechanism is left to the discussion
on the list. Its design section below sketches the trade-offs rather than settling on one.

### Feature 2: assign a task to more than one pool

Let a task declare a list of pools, each with its own slot requirement, and gate the task on them
together. Two combining modes are supported:

```python
from airflow.sdk import PoolSlot

# All (AND): needs a db connection and GPU capacity at the same time
@task(pools=[PoolSlot("db_connections", 1), PoolSlot("gpu_pool", 2)], pool_mode="all")
def run_model():
    ...

# Any (OR): runs on whichever interchangeable GPU cluster has room
@task(pools=[PoolSlot("gpu_east", 2), PoolSlot("gpu_west", 2)], pool_mode="any")
def run_model_either():
    ...
```

- **All (`pool_mode="all"`, the default)**: the task runs only when *every* listed pool has enough
  free slots. When it runs it takes the requested slots from each pool, and releases them all when it
  finishes. Use this when a task genuinely consumes several scarce resources at once.
- **Any (`pool_mode="any"`)**: the task runs as soon as *at least one* listed pool has enough free
  slots. It takes the requested slots from a single pool that has room, and releases them when it
  finishes. Use this when a task can run against any of several interchangeable resources and should
  take whichever is free.

A single string keeps working and is treated as a list of one, for which the mode does not matter.

### Feature 3: reserve slots in a pool for high priority tasks

Let a pool set aside part of its slots for high priority work, so that a backlog of ordinary tasks
cannot occupy the whole pool. A pool gains one setting: how many of its slots are reserved.

```text
pool "compute" has 10 slots, 2 reserved for the highest priority tasks
```

Ordinary scheduling is held to the unreserved 8 slots; the 2 reserved slots are released only to the
highest priority candidates waiting for the pool. A pool with 0 reserved slots behaves exactly as
pools do today.

There is deliberately no configured priority cutoff. The scheduler already considers candidates in
`priority_weight` order, highest first, so "high priority" means whichever tasks sort highest among
those competing for the pool, not tasks above a fixed number. This avoids asking an administrator to
pick a threshold against a `priority_weight` scale that authors do not share and that the scheduler
computes from the weight rule rather than taking the author's integer verbatim (see the note on
terms). The trade-off — that a pool whose queue is entirely lower priority work will still fill its
reserved slots, because nothing in that queue is "high" in absolute terms — is called out as an open
question.

This is different from `priority_weight` on its own. Today `priority_weight` only changes the order
tasks are looked at. Once ordinary tasks have taken every slot, a high priority task waits until one
of them finishes, however important it is. Reserving slots keeps a floor of capacity that ordinary
tasks cannot take, so a high priority task can move into a reserved slot as soon as one frees up.

The concrete problem this solves: a pool of 10 with a burst of ordinary tasks and a few high priority
tasks. Today, if the ordinary tasks are examined first they take all 10 slots and the high priority
tasks wait. With 2 slots reserved, ordinary tasks are held to 8 and the 2 reserved slots stay
available for the highest priority tasks, so they are never fully crowded out.

This reservation is a soft floor, not a hard guarantee. Airflow does not stop a running task to free a
slot (it has no preemption, and AIP-100 does not add it). Reserved slots guarantee that high priority
tasks get free slots as ordinary tasks finish, not that ordinary tasks are killed to make room
immediately.

This feature changes the same scheduling code that AIP-100 is reworking, so its design needs to be
coordinated with that effort. See its design section below.

## Design

All three features share one rule: the scheduler must be able to read a concrete pool name and slot
count from the database when it runs its critical section. Nothing here asks the scheduler to run user
code or render templates.

### Feature 1 design

#### Phase 1

The key insight is that the value can be produced by a worker, where running user code is normal, and
then read by the scheduler as plain data. This is how dynamic task mapping already works: an upstream
task writes a value, and the scheduler reads a small piece of state back to decide how many mapped
instances to create, without ever running the mapping code itself.

1. The upstream task runs on a worker and stores its return value as a normal XCom in the core XCom
   `value` column, which holds plain JSON.
2. When the downstream task's dependencies are satisfied, the scheduler reads that stored value and
   writes the result into the `TaskInstance.pool` column, before the task moves to the scheduled
   state. This happens in `DagRun._get_ready_tis`, alongside the mapped task expansion step, where
   the dependency check has already confirmed the upstream task finished and committed its XCom. It
   mirrors how mapped task length is read today (`get_task_map_length`, backed by the `TaskMap`
   table).
3. From there the scheduler's critical section reads a concrete `pool` value exactly as it does today.
   Slot accounting and the row locking are untouched.

The security boundary is the one place that needs care, and it is sharper than "read plain JSON."
Airflow's XCom deserialization path decodes the stored JSON with a decoder that can reconstruct
registered Airflow classes from the payload — that is framework code, but it is driven by data an
upstream task controls, so the scheduler must not go through it. Instead the scheduler reads the
stored value as a raw string and validates that it *is* a plain string naming a pool, failing the
task with a clear message otherwise. It never hands the value to the XCom deserializer, and it never
reconstructs objects from it. This keeps the scheduler free of user code even though the value
originates from user code on a worker. (An alternative that avoids the value column entirely is to
have the worker write the resolved name into a dedicated scalar column, exactly as mapped task length
is stored in `TaskMap.length`; the scheduler would then read a plain string it never has to decode.)

Custom XCom backends are a further concern. A custom backend stores values elsewhere (S3, GCS, a
pickled object) and reads them back by running its own deserialization code — user code the scheduler
must not run. The backend is a single deployment-wide setting with no per-row marker, so the
restriction is applied at deployment granularity: when a custom XCom backend is configured, dynamic
pools are disabled with a clear message. Whether to support custom backends at all is an open
question below.

Serialization stores a dynamic pool by reference, not by value: the upstream task id and key, never
any code. Airflow already serializes a task's output (an `XComArg`) exactly this way — as a
`{task_id, key}` reference that is rebuilt after the Dag loads — so a pool set to a task output rides
that existing path rather than needing a new encoding. A plain string is stored as it is today.
Templating the pool field remains blocked for Phase 1: the serializer forbids templating any
`BaseOperator.__init__` argument except `email`, and `pool` is one of those arguments.

The database needs a way to tell "pool not resolved yet" apart from a resolved pool, so the scheduler
knows to wait for the upstream value. The `pool` column is not nullable today, so this needs either a
new task instance state or a nullable column, and a database migration. This is the largest single
piece of the work.

There is one subtlety behind the "no explicit `>>` needed" behaviour above. A task's output is an
`XComArg`, but auto-created dependencies today are wired from an operator's template fields, and
`pool` is deliberately not templatable — so a task output passed to `pool` would *not* create the
scheduling edge through the existing mechanism. Making the edge automatic therefore requires new
dependency wiring specifically for the pool argument, rather than reuse of the template-field path.
This is real work, not a free ride on the existing machinery, and is called out under the API
surface.

#### Phase 2

Phase 2 relaxes the Phase 1 rule that `pool` can never be a template. The block exists only because
the scheduler cannot render templates; if the template is rendered into a concrete value *before*
the scheduler reads the pool, the scheduler still sees a plain string and the constraint holds. The
open question is where that rendering happens, and each option has a different cost:

- **At task-instance creation**, from run-static context. This is the cheapest: the values
  (`params`, `dag_run.conf`, `ds`) are known when the run starts, so no upstream task, XCom, or new
  task-instance state is needed. It covers only templates that do not reference another task's output.
  A deployment can already approximate this today with a task instance mutation hook — see
  Alternative A.
- **An auto-generated producer task** that renders the string and publishes the result, reusing the
  Phase 1 read path. This removes the manual task but inherits two hard problems: a template's
  dependency on an upstream XCom generally cannot be discovered from the template text, so the
  generated task cannot always know what to wait for; and inserting a task node the author did not
  write is new behaviour that raises UI, stable-identifier, and serialization questions.
- **Rendering on a worker or the triggerer.** Both may run user code, but both sit at or after the
  point where a task is already scheduled, which is the same ordering problem that rules out the
  triggerer for Phase 1 (see Alternative B).

Because these options differ so much in cost and coverage, Phase 2 does not commit to one here; the
choice is left to the discussion.

### Feature 2 design

A task instance today has one `pool` and one `pool_slots` column, with no foreign key to the pool
table — the pool is held by name. Supporting several pools means storing a set of pool name and slot
count pairs per task instance in a side table linked to the task instance, rather than two columns.
Because this replaces a core, heavily read column, it is not a purely additive migration: it needs a
backfill and a compatibility decision for everything that reads `TaskInstance.pool` (the REST API,
CLI, and UI). That is covered under Considerations.

The critical section already locks every pool's row for the duration of its work
(`Pool.slots_stats` with row locking), so it can check and decrement several pools together without
the risk of a task grabbing one pool and then blocking forever waiting for another. The per task loop
that today reads one pool becomes a loop over the task's pools, and the combining mode decides the
rule:

- **All (AND)**: the task is schedulable only if every listed pool has room; slots are then taken
  from all of them together. If any pool is full the task waits and no slots are taken from the
  others.
- **Any (OR)**: the task is schedulable as soon as one listed pool has room; slots are taken from a
  single pool that has room (evaluated in listed order) and from that pool only.

Slots are decremented in memory against the already-locked pool rows, so no pool row is ever
re-locked inside the loop and there is no lock-ordering deadlock. In multi-team deployments the
scheduler already skips a task whose Dag team does not own the pool it targets; this check must run
for *every* pool in the side table, so that a task cannot draw slots from another team's pool through
a secondary entry.

The author-facing API needs a precedence rule between the existing `pool`/`pool_slots` arguments and
the new `pools` list; that is covered under the API surface.

### Feature 3 design

The `Pool` model gains one setting: a reserved slot count, defaulting to 0 (no reservation, today's
behaviour). The scheduler's pool statistics carry the reserved count alongside the existing totals.

The fit with the scheduler is close, because the critical section already has what it needs. It walks
candidate tasks in `priority_weight` order, highest first, and `priority_weight` is already loaded in
the loop. The reservation is a floor on the ordinary admission check: a task may take a slot only
while the pool's free slots exceed the reserved count, so ordinary scheduling is held to the
unreserved portion and the reserved slots stay open. Because candidates are processed highest
priority first and slots are decremented as the scheduler goes, the reserved slots are reached only
by the highest priority tasks still waiting — no configured cutoff is involved.

Two implementation points matter and are more than a one-line change:

- The admission loop is global across pools, so the reserved count and the running free-slot figure
  live in the per-pool statistics the scheduler already computes, not in a loop-local.
- A task deferred because only reserved slots remain must be recorded as a *starved task*, not a
  *starved pool*. Marking the whole pool starved would wrongly exclude higher priority tasks that
  appear in a later scheduling cycle from ever reaching the reserved slots. A task that can never fit
  (its slot request exceeds the unreserved portion and it is not among the highest priority) must be
  failed visibly rather than left waiting forever.

No preemption is involved. The check only governs whether a task may occupy a slot that is already
free; it never frees an occupied slot. This keeps the change confined to the admission check and
leaves running tasks alone.

Because `priority_weight` is set by Dag authors, the reservation is a soft policy within a trust
domain: any author can raise a task's weight to reach the reserved slots. It is not an enforceable
guarantee across mutually distrusting teams. This is noted under Considerations.

This feature changes the exact code AIP-100 is reworking, so the two need to be coordinated. AIP-100
does not itself propose reserved slots; it approaches starvation by reordering and aging rather than
by reserving capacity, and it considered and set aside reservation-style ideas (priority bands, a
quota system). Two interactions matter:

- AIP-100's dynamic priority and aging still check limits on every scheduling cycle, which is
  compatible with a reserved portion being one of those checks.
- An idea raised in AIP-100's discussion — only creating task instances that can already run, so that
  any scheduled task is guaranteed a slot — is in tension with a reservation that is decided at
  scheduling time rather than creation time (and with Feature 1, where the pool is not known at
  creation time). If AIP-100 takes that direction, reserved slots and Feature 1 both need to be
  treated as scheduling time decisions rather than creation time ones.

Coordinating early with the AIP-100 effort is recommended, since both touch the admission logic in the
critical section. AIP-100 is an in-progress, contested draft as of 2026-07, so these characterizations
should be re-checked against its final accepted form.

## Alternatives considered

### Alternative A for Feature 1: a declarative mapping from run configuration to pool

Instead of taking the pool from an upstream task's output, let the author declare a fixed mapping from
run configuration or parameters to a pool name, resolved by a simple lookup:

```python
@task(pool=PoolFromConf(key="pool", default="default_pool"))
def run_model():
    ...
```

The scheduler resolves this with a plain dictionary lookup on the run's configuration when it builds
the task instance, with no upstream dependency and no XCom read. This is lighter than the upstream
value approach: no new task instance state, no migration, and no XCom backend concern. It fully
covers the case where the pool is decided by whoever triggers the run, and it overlaps heavily with
the "render at task-instance creation" option for Phase 2.

It does not cover the main use case, where the pool depends on something computed during the run. The
run's configuration is fixed when the run starts. For that reason this is presented as a smaller,
complementary capability rather than the whole of Feature 1. It could ship first on its own and would
exercise much of the same serialization and resolution plumbing.

A deployment can already achieve a version of this today without any new feature, using a task
instance mutation hook in `airflow_local_settings.py`, which now receives the `DagRun` and so can read
its configuration ([PR #68198](https://github.com/apache/airflow/pull/68198)). That path is for
deployment administrators, not Dag authors, and is worth documenting regardless of what this AIP does.

### Alternative B for Feature 1: render the pool on the triggerer and have the scheduler read the result

Since the triggerer is allowed to run user code, one idea is to have it render the pool for a task and
write the result somewhere the scheduler can read it. This turns out not to fit the architecture.

A trigger only ever exists after a task has already been scheduled. Either a running task calls
`self.defer()` during execution, which creates the trigger and puts the task into the deferred state,
or a task with `start_from_trigger` set begins on the triggerer — but by then the scheduler has
already created the task instance with its pool set. Nothing creates a trigger for a task that has not
been scheduled yet. So the triggerer sits after the point where the pool is needed, not before it.

The triggerer can render templates, which it does for `start_from_trigger` tasks, so rendering is not
the obstacle. Ordering is. Making this work would require a new pre-scheduling state, a new way to
create triggers before scheduling, a new way to write the rendered pool back (today a fired trigger
only writes back the fields needed to resume the task), and a way for the scheduler to pick the task
back up afterwards. This is a large amount of new machinery on the scheduling hot path, to reach the
same result the main proposal reaches by having the worker produce the value and the scheduler read it
as data. This alternative is not recommended.

### Alternative for Feature 2: keep combining pools by hand

Users can approximate multiple pools today by creating one combined pool per combination of resources.
This needs no changes, but it couples unrelated limits and multiplies the number of pools, since every
new combination needs its own pool. It also cannot express the "any" case at all. The proposal exists
because this workaround does not scale.

### Alternative for Feature 3: pool groups with overcommit

An earlier version of this proposal solved the reservation problem with pool groups: several pools
share a named group that caps their combined slots, and a sub pool may be sized larger than the group
so it can borrow the shared capacity. For the priority case this means a priority pool and a regular
pool in a group of 10, with the regular pool capped at 8, so priority work can use up to 10 while
regular work is held to 8.

This works, but it is a less direct fit for the problem. It models one resource as two pools plus an
umbrella object, and it caps regular work at a ceiling rather than guaranteeing priority work a floor.
Reserved slots on a single pool express the same intent with one pool and one resource, give priority
work a real floor, and reuse the ordering the scheduler already applies. A shared slot budget across
several genuinely distinct pools is a reasonable future feature (a broadly related request is
[#35689](https://github.com/apache/airflow/issues/35689), which asks for assigning a pool to a task
group), but it is not needed to solve the priority problem and is left out here.

## Out of scope

- **Dynamic slot counts.** Feature 1 resolves the pool name at run time but keeps the slot count
  fixed. Resolving the slot count the same way is a natural follow up (asked for in
  [#35803](https://github.com/apache/airflow/issues/35803)), but a changing slot count interacts with
  slot accounting and with the AIP-100 work, so it is left out for now.
- **A pool value that is more than a single upstream string.** Feature 1 takes one upstream value that
  resolves to one pool name. Expressions that combine several values, or that pick a pool per mapped
  index (asked for in [#33657](https://github.com/apache/airflow/issues/33657)), are follow ups.
- **Custom XCom backends for Feature 1.** Reading a value from a custom backend would mean running the
  backend's code in the scheduler. The initial version restricts dynamic pools to the core XCom
  backend. Whether to support custom backends later is an open question.
- **Pool groups.** A shared slot budget across several distinct pools is a reasonable future feature,
  but it is not needed to solve the priority problem that Feature 3 addresses, so it is left out of
  this AIP.
- **Preemption.** Reserved slots are a soft floor. Stopping a running task to free a slot for a high
  priority task is not proposed. Airflow has no preemption today and AIP-100 does not add it.

## Considerations

### What problem does it solve?

It lets tasks match real resource limits more closely: choosing a pool based on what a run actually
needs, gating a task on more than one scarce resource (in either an all or any relationship), and
keeping capacity available for important work. Today all of these require workarounds that couple
unrelated limits or cannot be expressed at all.

### Which users are affected by the change?

Dag authors gain new optional ways to declare pools. Deployment administrators who manage pools gain
reserved slots to configure. Users who do nothing new are unaffected: a plain string pool and a fixed
slot count behave exactly as before.

### How are users affected by the change?

Feature 1 needs a database migration (a way to mark an unresolved pool) and, for Phase 2, relaxing the
block on templating the pool field so it can be rendered before scheduling. Feature 2 replaces the
`pool`/`pool_slots` columns with a side table, which is a structural change to a core table: it needs
a backfill, a compatibility decision for the REST API, CLI, and UI that read those columns, and a
downgrade path. Feature 3 adds one setting to the pool table, which is additive. Existing Dags need no
changes. The UI needs to show the resolved pool for Feature 1 alongside the declared value, so that
what actually ran is visible, and the Pool admin surface and `airflow pools` CLI gain the reserved
slot count for Feature 3.

### API surface

- **Feature 1**: `pool` accepts a task output in addition to a string. Because `pool` is str-typed and
  deliberately not templatable, passing a task output must automatically create the upstream edge
  through dedicated dependency wiring (not the template-field path), so the author writes no explicit
  `>>`. It also needs a decision on how to keep static analysis useful for the common string case — for
  example a distinct spelling such as `pool=Pool.resolve(picked)` that keeps `pool: str` for literals
  and gives the dynamic form its own type. The resolved value is validated as a plain string naming a
  pool.
- **Feature 2**: a new `pools` argument taking a list of `PoolSlot`-like entries plus a `pool_mode`
  of `all` or `any`. Setting both `pool`/`pool_slots` and `pools` should be a clear error rather than
  a silent precedence rule. The name of the per-pool entry should read as a requirement (how many
  slots this task needs from a pool), not as a single unit of capacity.
- **Feature 3**: a reserved slot count on the pool, exposed through the Pool REST endpoints, the
  `airflow pools` CLI, and the Pools admin UI.

### Are there any downsides to this change?

All three features touch the scheduler's locked critical section, which is a known throughput
bottleneck. Feature 1 adds an XCom read and a write into `TaskInstance.pool` on the ready-task path;
Feature 2 turns a single-column read into a side-table lookup and locks and decrements several pool
rows per task; Feature 3 adds a per-candidate condition to the admission loop. Each adds work per
scheduling cycle and to the time pool rows are held locked. The columns the side table is filtered on
must be indexed, and scheduler throughput should be benchmarked before and after. These costs are the
main risk and are called out so they can be measured rather than discovered in review.

### What is the level of migration effort needed for users?

None for existing Dags. The new capabilities are opt in. Deployment administrators who want reserved
slots set those up once per pool. The Feature 2 column change is handled by the shipped migration, not
by users.

### Are there security concerns?

Feature 1 is the main one. The scheduler must read the resolved pool value as a raw string and
validate it, never passing it through the XCom deserializer (which can reconstruct registered classes
from user-influenced data) and never running a custom XCom backend's code. The proposal reads the core
XCom value directly and validates it is a plain string, and disables dynamic pools when a custom
backend is configured. A second consideration is that a runtime-resolved pool name is no longer
visible in the Dag source, so deployments that review which pools a Dag may target lose that
parse-time check; the multi-team pool ownership check still runs on the resolved value. Feature 3 runs
no user code, but its reservation is gated on author-set `priority_weight`, so it is a soft policy
within a trust domain rather than a cross-team guarantee.

### Testing strategy

Beyond unit coverage of each new behaviour, the changes to the critical section need concurrency tests
around multi-pool slot accounting (Feature 2) and the reserved-slot floor (Feature 3), migration
up/down and backfill tests (Features 1 and 2), resolution-timing tests for the unresolved-pool state
and the not-found and wrong-type failure paths (Feature 1), and a scheduler throughput benchmark to
catch regressions on the hot path.

### Open questions

1. Should Feature 1 support custom XCom backends, or stay restricted to the core backend? This bounds
   how widely it can be used.
2. When a task with a dynamic pool is cleared and rerun, should the pool be resolved again from the
   new upstream value, or should the first resolved value be kept? The critical section assumes the
   pool is stable within a run attempt, so this needs to be settled in the design.
3. What should happen when a resolved or declared pool does not exist, or belongs to another team?
   Today the task waits indefinitely (only a scheduler log warning is emitted each cycle; nothing
   surfaces to the user and the task is never failed). A visible task failure would be better, and is
   needed for Feature 1 so that a mistyped or wrong-typed resolved value fails fast.
4. For a mapped task, should every mapped instance share one resolved pool, or should each mapped
   index resolve its own?
5. Should reserved slots (Feature 3) live in pools at all, or is that better handled by the AIP-100
   priority work? This needs agreement with that effort.
6. Reserved slots go to the highest priority candidates rather than to tasks above a configured
   cutoff. This needs no threshold to reason about and no shared priority scale, but it means a pool
   whose queue is entirely lower priority work will still fill its reserved slots — the reservation
   only bites when higher priority work is competing. Is that soft behaviour sufficient, or is a
   configurable cutoff also wanted (accepting that a cutoff is only meaningful under the absolute
   weight rule)?
7. For Phase 2 of Feature 1, where should a templated pool string be rendered — at task-instance
   creation, by an auto-generated producer task, or on a worker/triggerer? Each covers a different set
   of templates at a different cost.

### What defines this AIP as done?

Each feature can be considered done independently. Feature 1 Phase 1 is done when a task's pool can be
set from an upstream task's output, the scheduler resolves it from the core XCom data without running
user code, the resolved pool is visible in the UI, and the behaviour is covered by tests including the
unresolved-pool state and the not-found and wrong-type failure paths. Phase 2 is done when a templated
pool string is resolved into a concrete pool before scheduling by the mechanism agreed on the list.
Feature 2 is done when a task can be gated on several pools at once in both all and any modes, with the
per-pool team check preserved. Feature 3 is done when a pool can reserve slots for the highest priority
tasks, its design has been coordinated with the AIP-100 effort, and the behaviour is covered by tests.
