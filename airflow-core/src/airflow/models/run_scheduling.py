# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Per-run values for a task's pool, pool_slots, priority_weight and queue (AIP-117).

For each field a task instance takes the first usable value in this order; the
``task_instance_mutation_hook`` cluster policy runs afterwards and has the last word:

1. the run's ``overrides.tasks`` entry for the task
2. the ``overrides.tasks`` entry for the nearest TaskGroup containing the task, then outer groups
3. the run's ``overrides.pools`` remap (pool only)
4. the task's Lookup
5. the value written in the Dag (which is the Airflow default when nothing was written)

An override is usable only while its target is nominated: a pool with ``allow_overrides`` set,
or a queue listed in ``[core] override_target_queues``. A run created by a request is rejected
if any requested value is unusable. Later writes (mapped expansion, clear, retry) check again
and fall back to the next value instead, so un-nominating or deleting a pool never leaves a
task instance that cannot be scheduled.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import attrs
import structlog
from sqlalchemy import select

from airflow.configuration import conf
from airflow.models.dag import DagModel
from airflow.models.pool import Pool
from airflow.serialization.definitions.lookup import coerce_field_value
from airflow.serialization.definitions.notset import ArgNotSet
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.definitions.mappedoperator import Operator

log = structlog.get_logger(__name__)

# Runs the scheduler creates have nobody to return an error to.
LENIENT_RUN_TYPES = frozenset({DagRunType.SCHEDULED, DagRunType.ASSET_TRIGGERED})


class SchedulingValueRejected(ValueError):
    """A requested or looked-up scheduling value cannot be used for this run."""


def get_override_target_queues() -> frozenset[str]:
    raw = conf.get("core", "override_target_queues", fallback="")
    return frozenset(queue.strip() for queue in raw.split(",") if queue.strip())


@attrs.frozen
class _PoolInfo:
    slots: int
    allow_overrides: bool
    team_name: str | None


def _get_group_ids(task: Operator) -> list[str]:
    """Return the ids of the groups containing ``task``, nearest first."""
    group_ids = []
    group = task.task_group
    while group is not None:
        if group.group_id is not None:
            group_ids.append(group.group_id)
        group = group.parent_group
    return group_ids


class RunSchedulingResolver:
    """
    Resolve the scheduling values of every task of one run.

    Built once per run and reused for each of its task instances, so the cost grows with the
    number of tasks and not with the number of mapped task instances.

    :param strict: raise :class:`SchedulingValueRejected` on the first unusable value instead of
        falling back. Used when a run is created by a request.
    """

    def __init__(
        self,
        *,
        dag: SerializedDAG,
        run_type: str,
        params: Mapping[str, Any] | None,
        overrides: Mapping[str, Any] | None,
        session: Session | None,
        strict: bool = False,
    ) -> None:
        self.dag = dag
        self.run_type = str(run_type)
        self.params = params
        self.task_overrides: Mapping[str, Mapping[str, Any]] = (overrides or {}).get("tasks") or {}
        self.pool_remap: Mapping[str, str] = (overrides or {}).get("pools") or {}
        self.session = session
        self.strict = strict
        self._pools: dict[str, _PoolInfo] | None = None
        self._dag_team: str | None | ArgNotSet = ArgNotSet()

    @property
    def has_overrides(self) -> bool:
        return bool(self.task_overrides or self.pool_remap)

    def _get_pools(self) -> dict[str, _PoolInfo]:
        if self._pools is None:
            if self.session is None:
                # Nothing can be checked, so no override is usable.
                self._pools = {}
            else:
                rows = self.session.execute(
                    select(Pool.pool, Pool.slots, Pool.allow_overrides, Pool.team_name)
                )
                self._pools = {name: _PoolInfo(slots, bool(allow), team) for name, slots, allow, team in rows}
        return self._pools

    def _get_dag_team(self) -> str | None:
        if isinstance(self._dag_team, ArgNotSet):
            if conf.getboolean("core", "multi_team") and self.session is not None:
                self._dag_team = DagModel.get_team_name(self.dag.dag_id, session=self.session)
            else:
                self._dag_team = None
        return self._dag_team

    def _find_pool(self, name: str) -> _PoolInfo | None:
        info = self._get_pools().get(name)
        if info is None:
            return None
        if info.team_name is not None and info.team_name != self._get_dag_team():
            return None
        return info

    def _is_override_target(self, pool: str) -> bool:
        info = self._find_pool(pool)
        return info is not None and info.allow_overrides

    def _fits(self, pool: str, slots: int) -> bool:
        info = self._find_pool(pool)
        return info is None or info.slots < 0 or slots <= info.slots

    def _reject(self, task: Operator, field: str, value: Any, reason: str) -> None:
        log.info(
            "scheduling value not used",
            dag_id=self.dag.dag_id,
            task_id=task.task_id,
            field=field,
            value=value,
            reason=reason,
        )
        if self.strict:
            # One message for every reason, so the response does not reveal which pools exist,
            # which are nominated, or which belong to another team.
            raise SchedulingValueRejected(
                f"{field}={value!r} cannot be used for task {task.task_id!r} in this run"
            )

    def validate_override_keys(self) -> None:
        unknown = [
            key
            for key in self.task_overrides
            if not self.dag.has_task(key) and key not in self.dag.task_group_dict
        ]
        if unknown:
            raise SchedulingValueRejected(f"overrides name unknown tasks or groups: {sorted(unknown)}")
        for source, target in self.pool_remap.items():
            if not self._is_override_target(target):
                log.info("pool remap target not usable", dag_id=self.dag.dag_id, source=source, target=target)
                raise SchedulingValueRejected(f"pool {target!r} cannot be used for this run")

    def _get_override_entries(self, task: Operator) -> list[Mapping[str, Any]]:
        keys = [task.task_id, *_get_group_ids(task)]
        return [self.task_overrides[key] for key in keys if key in self.task_overrides]

    def get_values(self, task: Operator) -> dict[str, Any]:
        """Return the fields of ``task`` whose value in this run differs from the Dag's."""
        values: dict[str, Any] = {}
        for field, lookup in (getattr(task, "scheduling_lookups", None) or {}).items():
            value = lookup.resolve(field, params=self.params, run_type=self.run_type)
            if isinstance(value, ArgNotSet):
                continue
            # A weight from a lookup replaces the final weight, as if weight_rule were absolute.
            if field == "priority_weight" or value != getattr(task, field):
                values[field] = value
        if self.strict:
            self._check_lookup_values(task, values)
        if self.has_overrides:
            self._apply_overrides(task, values)
        return values

    def _check_lookup_values(self, task: Operator, values: dict[str, Any]) -> None:
        pool = values.get("pool", task.pool)
        if "pool" in values and self._find_pool(pool) is None:
            self._reject(task, "pool", pool, "pool does not exist or belongs to another team")
        if "pool_slots" in values and not self._fits(pool, values["pool_slots"]):
            self._reject(task, "pool_slots", values["pool_slots"], f"more than the slots of {pool!r}")

    def _apply_overrides(self, task: Operator, values: dict[str, Any]) -> None:
        entries = self._get_override_entries(task)
        base_pool = values.get("pool", task.pool)
        pool_candidates = [entry["pool"] for entry in entries if "pool" in entry]
        if base_pool in self.pool_remap:
            pool_candidates.append(self.pool_remap[base_pool])
        for candidate in pool_candidates:
            if self._is_override_target(candidate):
                values["pool"] = candidate
                break
            self._reject(task, "pool", candidate, "pool is not nominated for overrides")

        pool = values.get("pool", base_pool)
        for field in ("pool_slots", "priority_weight", "queue"):
            for entry in entries:
                if field not in entry:
                    continue
                value = coerce_field_value(field, entry[field])
                if isinstance(value, ArgNotSet):
                    self._reject(task, field, entry[field], "not a valid value")
                elif field == "queue" and value not in get_override_target_queues():
                    self._reject(task, field, value, "queue is not listed in [core] override_target_queues")
                elif field != "queue" and not self._is_override_target(pool):
                    self._reject(task, field, value, f"the task's pool {pool!r} is not nominated")
                elif field == "pool_slots" and not self._fits(pool, value):
                    self._reject(task, field, value, f"more than the slots of {pool!r}")
                else:
                    values[field] = value
                    break


def validate_run_scheduling(
    dag: SerializedDAG,
    *,
    run_type: DagRunType,
    params: Mapping[str, Any],
    overrides: Mapping[str, Any] | None,
    session: Session,
) -> None:
    """
    Check the values a new run would give its tasks, before the run is written.

    :raises SchedulingValueRejected: for a run created by a request. A run the scheduler creates
        is never rejected: its unusable values are logged and fall back.
    """
    if not overrides and not any(
        getattr(task, "scheduling_lookups", None) for task in dag.task_dict.values()
    ):
        return
    if overrides and run_type in LENIENT_RUN_TYPES:
        raise SchedulingValueRejected("overrides can only be set on runs triggered through the API or UI")
    resolver = RunSchedulingResolver(
        dag=dag,
        run_type=run_type,
        params=params,
        overrides=overrides,
        session=session,
        strict=run_type not in LENIENT_RUN_TYPES,
    )
    if resolver.has_overrides:
        resolver.validate_override_keys()
    for task in dag.task_dict.values():
        resolver.get_values(task)
