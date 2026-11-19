<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [AIP-117 proof of concept: demo](#aip-117-proof-of-concept-demo)
  - [What this branch implements](#what-this-branch-implements)
  - [Where `dag.param()` differs on these fields](#where-dagparam-differs-on-these-fields)
  - [Not in this branch](#not-in-this-branch)
  - [Running the demo](#running-the-demo)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# AIP-117 proof of concept: demo

[AIP-117](https://cwiki.apache.org/confluence/spaces/AIRFLOW/pages/446071748/AIP-117+Per-Run+Task+Scheduling+Controls)
lets a Dag author take a task's `pool`, `pool_slots`, `priority_weight` or `queue` from a bounded
param or the run type, and lets a user who can edit a Dag override them for one run, but only
towards pools and queues an administrator nominated.

## What this branch implements

- The four fields accept the existing `dag.param(name)` or the new helper
  `by_run_type(**values, default=...)` from `airflow.sdk`, in task arguments, in `.partial()` and
  in Dag or TaskGroup `default_args`. There is no new class in the public API.

  ```python
  with DAG(
      "regional_sync",
      params={"warehouse_pool": Param("wh_small", enum=["wh_small", "wh_large"])},
  ) as dag:
      dag.default_args["pool"] = dag.param("warehouse_pool")  # tasks created after this line
      load = LoadOperator(task_id="load", pool=dag.param("warehouse_pool"))
      merge = MergeOperator(task_id="merge", pool=by_run_type(backfill="backfill_pool", default="etl_pool"))
      fan = FanOperator.partial(task_id="fan", pool=dag.param("warehouse_pool")).expand(x=[1, 2, 3])
  ```

- The Dag processor rejects a param that the Dag does not declare, a param without a string `enum`
  or `const` (for `pool` and `queue`) or without an integer `enum` or type `integer` with `minimum`
  and `maximum` (for `pool_slots` and `priority_weight`), an unknown run type, and `by_run_type()`
  without `default=`. Reading `task.pool` while parsing gives the value a run without conf would
  get, and `dag.params["x"]` still gives the param's default.
- An `overrides` field on `POST /api/v2/dags/{dag_id}/dagRuns` and
  `POST /api/v2/assets/{asset_id}/materialize`: `tasks` keyed by task or TaskGroup ID, and `pools`
  to move every task of the run off one pool. It needs Dag edit permission, a pool with
  `allow_overrides` set (new pool field), or a queue listed in `[core] override_target_queues`. An
  unknown, unnominated or other-team pool gets the same error.
- Values are applied at every write of a task instance: creation (both the bulk insert path and the
  mutation hook path), mapped expansion, clear and retry. Overrides are checked again each time and
  fall back to the next value if their target was un-nominated. `task_instance_mutation_hook` runs
  last.
- UI: a *Scheduling Overrides* section in the trigger form (task or group, nominated pools and
  queues only, slots, priority, and pool moves), an *Allow Overrides* checkbox and *Override target*
  badge for pools, and the run's overrides on the Dag run *Details* tab. The Dag run API returns
  them as `overrides`.
- A migration adding `dag_run.scheduling_overrides` (deferred JSON), `dag_run.has_scheduling_overrides`
  and `slot_pool.allow_overrides`.

## Where `dag.param()` differs on these fields

- On a templated field, `dag.param("x")` is resolved on the worker by `DagParam.resolve()`, which
  reads the unvalidated `dag_run.conf`. On these four fields the scheduler reads the run's params
  after conf is merged and validated, so the same expression has two resolution paths.
- `dag.param()` needs the Dag object, so it cannot go in the `default_args` passed to `DAG(...)` or
  `@dag(...)`. Set `dag.default_args["pool"]` inside the `with` block; it applies to tasks created
  after that line. `by_run_type()` works in both places.
- Inside an `@dag` function the arguments are already `DagParam` objects, so `pool=warehouse_pool`
  works when `params` declares the bound. The argument's default replaces the one in `params`.
- `dag.param("x", default)` writes `default` into the Dag's param, for every task and for the
  trigger form. Two calls with different defaults are rejected for these fields, because a run
  without conf can only get one of them.
- Only the Dag-level param counts. A task-level `params={"x": ...}` does not change these fields.

## Not in this branch

An Airflow Variable as a source; translating one param into several fields (the earlier `.map()`);
"sets pool for N tasks" notes under these params in the trigger form; showing where a value came
from on the task instance page; a Dag warning and fallback when a scheduled run's value names a
missing pool; rejecting `by_run_type()` on other fields; the `queue_to_coordinator` runtime check;
docs.

## Running the demo

```bash
mkdir -p files/dags && cp dev/aip117_demo/dags/aip117_demo.py files/dags/
breeze start-airflow
# in another terminal, once the API server is up on http://localhost:28080 (admin / admin):
./dev/aip117_demo/demo.sh setup          # pools; heavy_pool is NOT nominated, etl_overflow_pool is
```

| Step | Command | What to show |
| --- | --- | --- |
| Capacity at trigger time | Trigger `aip117_regional_sync` from the UI | The `warehouse_pool` dropdown comes from the enum param |
| | `demo.sh trigger-small`, then `trigger-large` | Pools page: `wh_small` runs 2 of 18, `wh_large` runs 16 |
| Bounded choice | `demo.sh trigger-bad-conf` | A value outside the enum is a 400 |
| Bounded at parse time | `cp dev/aip117_demo/unbounded_param.py files/dags/` | Import error: the param must declare `enum` or `const` |
| Mapped tasks | `demo.sh trigger-fan`, then `demo.sh tis aip117_fan_out` | All three mapped task instances of `fan` use `wh_large` |
| Backfill isolation | `demo.sh backfill`, then `demo.sh pools-by-run` | Backfill runs use `backfill_pool`, the scheduled run `etl_pool` |
| Override needs nomination | `demo.sh override`, `demo.sh override-unknown` | Same 400 for an unnominated and a missing pool |
| | `demo.sh nominate`, or tick *Allow Overrides* when editing `heavy_pool` | The pool card gets an *Override target* badge |
| Override from the UI | Trigger `aip117_reprocess_orders`, open *Scheduling Overrides* | Only nominated pools are offered; set `extract` to `heavy_pool` with 4 slots and the `transform` group to `heavy_pool` |
| | Open the new run, *Details* tab, then task `extract` | The run shows its overrides next to conf; `extract` runs in `heavy_pool` with 4 slots |
| Move a run off a pool | *Move a pool* in the same section, or `demo.sh remap`, then `demo.sh tis` | Every `etl_pool` task of the run uses `etl_overflow_pool` |

`demo.sh` reads `AIRFLOW_API` (default `http://localhost:28080`) and `AIRFLOW_TOKEN` (default: a
token for `admin` / `admin`).
