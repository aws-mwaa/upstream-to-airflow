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
Take a task's scheduling fields from a value chosen when a run is created (AIP-117).

``pool``, ``pool_slots``, ``priority_weight`` and ``queue`` accept ``dag.param(name)`` or
``by_run_type(...)``. Both are kept on the task as the lookups below, next to the plain value a
run without conf would get, and the scheduler resolves them when it writes a task instance.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk.api.datamodels._generated import DagRunType
from airflow.sdk.definitions._internal.abstractoperator import (
    DEFAULT_POOL_NAME,
    DEFAULT_POOL_SLOTS,
    DEFAULT_PRIORITY_WEIGHT,
    DEFAULT_QUEUE,
)
from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
from airflow.sdk.definitions.param import DagParam

if TYPE_CHECKING:
    from airflow.sdk.definitions.dag import DAG

__all__ = ["LOOKUP_FIELDS", "by_run_type"]

LOOKUP_FIELDS: dict[str, type] = {"pool": str, "pool_slots": int, "priority_weight": int, "queue": str}
_FIELD_DEFAULTS: dict[str, Any] = {
    "pool": DEFAULT_POOL_NAME,
    "pool_slots": DEFAULT_POOL_SLOTS,
    "priority_weight": DEFAULT_PRIORITY_WEIGHT,
    "queue": DEFAULT_QUEUE,
}
_RUN_TYPES = frozenset(run_type.value for run_type in DagRunType)


def _is_int(value: Any) -> bool:
    # bool is an int subclass: a boolean param would otherwise become one pool slot.
    return isinstance(value, int) and not isinstance(value, bool)


def _is_usable(field: str, value: Any) -> bool:
    if LOOKUP_FIELDS[field] is str:
        return isinstance(value, str) and bool(value)
    return _is_int(value) and (field != "pool_slots" or value >= 1)


@attrs.frozen
class ParamLookup:
    """
    What ``dag.param(name)`` becomes on a scheduling field.

    The scheduler reads the value from the run's params, after conf is merged and validated. It
    never calls ``DagParam.resolve()``, which reads the unvalidated ``dag_run.conf``.
    """

    name: str
    dag_id: str
    declared_default: Any = NOTSET
    """The default given to ``dag.param(name, default)``, which also became the param's default."""

    @classmethod
    def from_dag_param(cls, dag_param: DagParam) -> ParamLookup:
        return cls(
            name=dag_param._name,
            dag_id=dag_param.current_dag.dag_id,
            declared_default=dag_param._default,
        )

    def serialize(self) -> dict[str, Any]:
        """
        Encode for the serialized Dag; ``airflow.serialization.definitions.lookup`` decodes it.

        ``sources`` is a list so that later source kinds, or a chain of sources, need no format
        change.
        """
        return {"sources": [{"kind": "param", "name": self.name}]}

    def get_parse_time_value(self, field: str, dag: DAG | None) -> Any:
        """Return the value a run without conf gets, for code that reads the field during parsing."""
        if dag is not None and self.name in dag.params:
            value = dag.params.get_param(self.name).value
            if _is_usable(field, value):
                return value
        return _FIELD_DEFAULTS[field]

    def validate_for(self, field: str, task_id: str, dag: DAG) -> None:
        """Reject a param that can never resolve or that leaves the choice unbounded."""
        where = f"{field!r} of task {task_id!r} uses dag.param({self.name!r})"
        if self.dag_id != dag.dag_id:
            raise ValueError(f"{where} of Dag {self.dag_id!r}, but the task is in Dag {dag.dag_id!r}")
        if self.name not in dag.params:
            raise ValueError(f"{where}, which the Dag does not declare in params")
        param = dag.params.get_param(self.name)
        if not isinstance(self.declared_default, ArgNotSet) and param.value != self.declared_default:
            # Every dag.param(name, default) call replaces the param's default for the whole Dag,
            # and the scheduler only sees the last one.
            raise ValueError(
                f"{where} with default {self.declared_default!r}, but the param's default is now "
                f"{param.value!r}; a run without conf would not get the default written here"
            )
        schema = param.schema or {}
        is_int = LOOKUP_FIELDS[field] is int
        choices = schema.get("enum")
        if choices is None and not is_int and "const" in schema:
            choices = [schema["const"]]
        # Anyone who can trigger the Dag can set its params, so the author must list the choices.
        if choices is not None:
            if bad := [choice for choice in choices if not _is_usable(field, choice)]:
                raise ValueError(f"{where}, whose choices {bad} are not valid here")
        elif (
            is_int
            and schema.get("type") == "integer"
            and _is_int(schema.get("minimum"))
            and _is_int(schema.get("maximum"))
        ):
            if not _is_usable(field, schema["minimum"]):
                raise ValueError(f"{where}, whose minimum is below 1")
        else:
            bound = "enum, or type 'integer' with minimum and maximum," if is_int else "enum or const"
            raise ValueError(
                f"{where}, which must declare {bound} "
                "so that whoever triggers a run chooses among values the Dag author listed"
            )


@attrs.frozen
class RunTypeLookup:
    """What ``by_run_type()`` returns: the value for each listed run type, and for all the others."""

    mapping: tuple[tuple[str, Any], ...]
    default: Any

    def serialize(self) -> dict[str, Any]:
        """Encode for the serialized Dag; ``airflow.serialization.definitions.lookup`` decodes it."""
        return {"sources": [{"kind": "run_type", "mapping": dict(self.mapping)}], "default": self.default}

    def get_parse_time_value(self, field: str, dag: DAG | None) -> Any:
        return self.default

    def validate_for(self, field: str, task_id: str, dag: DAG | None = None) -> None:
        where = f"{field!r} of task {task_id!r} uses by_run_type()"
        if not _is_usable(field, self.default):
            raise ValueError(f"{where} with default {self.default!r}, which is not valid here")
        if bad := {run_type: value for run_type, value in self.mapping if not _is_usable(field, value)}:
            raise ValueError(f"{where} with {bad}, which is not valid here")


def by_run_type(*, default: Any, **values: Any) -> RunTypeLookup:
    """
    Choose a task's ``pool``, ``pool_slots``, ``priority_weight`` or ``queue`` by the type of the run.

    .. code-block:: python

        MergeOperator(task_id="merge", pool=by_run_type(backfill="backfill_pool", default="etl_pool"))

    Reading the field while the Dag is parsed returns ``default``, so ``task_policy`` keeps seeing
    a plain value.

    :param values: the value for a run type, keyed by ``backfill``, ``scheduled``, ``manual``,
        ``operator_triggered``, ``asset_triggered`` or ``asset_materialization``.
    :param default: the value for every run type not in ``values``.
    """
    if not values:
        raise ValueError("by_run_type() needs at least one run type")
    if unknown := set(values) - _RUN_TYPES:
        raise ValueError(
            f"by_run_type() got unknown run types {sorted(unknown)}; expected some of {sorted(_RUN_TYPES)}"
        )
    return RunTypeLookup(mapping=tuple(values.items()), default=default)


def as_lookup(value: Any) -> ParamLookup | RunTypeLookup | None:
    """Return the lookup that a scheduling field's argument stands for, if it is one."""
    if isinstance(value, DagParam):
        return ParamLookup.from_dag_param(value)
    if isinstance(value, RunTypeLookup):
        return value
    return None


def split_lookup(field: str, value: Any, task_id: str) -> tuple[ParamLookup | RunTypeLookup | None, Any]:
    """
    Split a scheduling field's argument into its lookup, if it is one, and its parse-time value.

    A ``by_run_type()`` value is checked here. A ``dag.param()`` is checked in ``DAG.validate()``,
    once every param of the Dag is declared.
    """
    lookup = as_lookup(value)
    if lookup is None:
        return None, value
    if isinstance(lookup, RunTypeLookup):
        lookup.validate_for(field, task_id)
    dag = value.current_dag if isinstance(value, DagParam) else None
    return lookup, lookup.get_parse_time_value(field, dag)


def take_lookups(kwargs: MutableMapping[str, Any], task_id: str) -> dict[str, ParamLookup | RunTypeLookup]:
    """Replace the lookups among the scheduling fields of ``kwargs`` with their parse-time values."""
    lookups = {}
    for field in LOOKUP_FIELDS:
        lookup, plain = split_lookup(field, kwargs.get(field), task_id)
        if lookup is not None:
            lookups[field] = lookup
            kwargs[field] = plain
    return lookups


def deepcopy_default_args(default_args: Any) -> Any:
    """
    Deep-copy a TaskGroup's ``default_args`` without copying a ``dag.param()`` on a scheduling field.

    A ``DagParam`` holds its Dag, so copying it would copy the whole Dag and every task in it.
    """
    memo = {}
    if isinstance(default_args, Mapping):
        memo = {
            id(value): value
            for field in LOOKUP_FIELDS
            if isinstance(value := default_args.get(field), DagParam)
        }
    return copy.deepcopy(default_args, memo)
