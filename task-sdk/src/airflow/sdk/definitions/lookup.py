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
"""Take a task's scheduling fields from a value chosen when a run is created (AIP-117)."""

from __future__ import annotations

from collections.abc import Mapping
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

if TYPE_CHECKING:
    from airflow.sdk.definitions.dag import DAG

__all__ = ["LOOKUP_FIELDS", "Lookup"]

LOOKUP_FIELDS: dict[str, type] = {"pool": str, "pool_slots": int, "priority_weight": int, "queue": str}
_FIELD_DEFAULTS: dict[str, Any] = {
    "pool": DEFAULT_POOL_NAME,
    "pool_slots": DEFAULT_POOL_SLOTS,
    "priority_weight": DEFAULT_PRIORITY_WEIGHT,
    "queue": DEFAULT_QUEUE,
}
_RUN_TYPES = frozenset(run_type.value for run_type in DagRunType)


@attrs.frozen
class ParamSource:
    """Read the value from a Dag param of the run, after conf is merged and validated."""

    name: str


@attrs.frozen
class RunTypeSource:
    """Pick the value by the run's type."""

    mapping: tuple[tuple[str, Any], ...]


def _is_field_type(field: str, value: Any) -> bool:
    expected = LOOKUP_FIELDS[field]
    # bool is an int subclass: a boolean param would otherwise become one pool slot.
    return isinstance(value, expected) and not isinstance(value, bool)


@attrs.frozen
class Lookup:
    """
    A task's ``pool``, ``pool_slots``, ``priority_weight`` or ``queue``, chosen when a run is created.

    .. code-block:: python

        params = {"warehouse_pool": Param("wh_small", enum=["wh_small", "wh_large"])}
        MergeOperator(task_id="merge", pool=Lookup.param("warehouse_pool"))
        LoadOperator(task_id="load", pool=Lookup.run_type({"backfill": "backfill_pool"}, default="etl_pool"))

    Reading the field while the Dag is parsed returns the value a run without conf would get, so
    ``task_policy`` keeps seeing a plain value.
    """

    sources: tuple[ParamSource | RunTypeSource, ...]
    default: Any = NOTSET
    translation: tuple[tuple[Any, Any], ...] | None = None

    @classmethod
    def param(cls, name: str, *, default: Any = NOTSET) -> Lookup:
        """
        Take the value from the Dag param ``name``.

        The param must declare ``enum``, or ``minimum`` and ``maximum`` for ``pool_slots`` and
        ``priority_weight``, because anyone who can trigger the Dag can set it.

        :param default: used when the param has no usable value. The param's own default
            serves when this is not given.
        """
        if not isinstance(name, str) or not name:
            raise ValueError("Lookup.param() needs a param name")
        return cls(sources=(ParamSource(name),), default=default)

    @classmethod
    def run_type(cls, mapping: Mapping[str, Any], *, default: Any) -> Lookup:
        """
        Pick the value by run type, for example ``{"backfill": "backfill_pool"}``.

        :param default: used for run types not in ``mapping``.
        """
        if not mapping:
            raise ValueError("Lookup.run_type() needs at least one run type")
        if unknown := set(mapping) - _RUN_TYPES:
            raise ValueError(
                f"Lookup.run_type() got unknown run types {sorted(unknown)}; expected some of {sorted(_RUN_TYPES)}"
            )
        return cls(sources=(RunTypeSource(tuple(mapping.items())),), default=default)

    def map(self, mapping: Mapping[Any, Any]) -> Lookup:
        """Translate the looked-up value, for example ``Lookup.param("size").map({"small": 1, "large": 4})``."""
        if not mapping:
            raise ValueError("Lookup.map() needs at least one entry")
        return attrs.evolve(self, translation=tuple(mapping.items()))

    def serialize(self) -> dict[str, Any]:
        """
        Encode for the serialized Dag; ``airflow.serialization.definitions.lookup`` decodes it.

        ``sources`` is a list so that later source kinds, or a chain of sources, need no format
        change. ``map`` is a list of pairs because a param choice may not be a string.
        """
        encoded: dict[str, Any] = {
            "sources": [
                {"kind": "param", "name": source.name}
                if isinstance(source, ParamSource)
                else {"kind": "run_type", "mapping": dict(source.mapping)}
                for source in self.sources
            ]
        }
        if not isinstance(self.default, ArgNotSet):
            encoded["default"] = self.default
        if self.translation is not None:
            encoded["map"] = [list(pair) for pair in self.translation]
        return encoded

    def _translate(self, value: Any) -> Any:
        if self.translation is None:
            return value
        return dict(self.translation).get(value, NOTSET)

    def get_parse_time_value(self, field: str, dag: DAG | None) -> Any:
        """Return the value a run without conf gets, for code that reads the field during parsing."""
        if not isinstance(self.default, ArgNotSet):
            return self.default
        for source in self.sources:
            if isinstance(source, ParamSource) and dag is not None and source.name in dag.params:
                value = self._translate(dag.params.get_param(source.name).value)
                if _is_field_type(field, value):
                    return value
        return _FIELD_DEFAULTS[field]

    def validate_for(self, field: str, task_id: str, dag: DAG) -> None:
        """Reject a lookup that can never resolve or that leaves the choice unbounded."""
        where = f"{field!r} of task {task_id!r}"
        if field not in LOOKUP_FIELDS:
            raise ValueError(f"{where} cannot use a Lookup; only {sorted(LOOKUP_FIELDS)} can")
        if not isinstance(self.default, ArgNotSet) and not _is_field_type(field, self.default):
            raise ValueError(f"{where} has a Lookup default that is not a {LOOKUP_FIELDS[field].__name__}")
        for source in self.sources:
            if isinstance(source, ParamSource):
                self._validate_param_source(source, field, where, dag)
            else:
                if isinstance(self.default, ArgNotSet):
                    raise ValueError(f"{where} uses Lookup.run_type() without default=")
                for _, value in source.mapping:
                    if not _is_field_type(field, self._translate(value)):
                        raise ValueError(f"{where} maps a run type to {value!r}, which is not valid here")

    def _validate_param_source(self, source: ParamSource, field: str, where: str, dag: DAG) -> None:
        if source.name not in dag.params:
            raise ValueError(f"{where} looks up param {source.name!r}, which the Dag does not declare")
        schema = dag.params.get_param(source.name).schema or {}
        enum = schema.get("enum")
        # Anyone who can trigger the Dag can set its params, so the author must list the choices.
        if enum is not None:
            choices = [self._translate(choice) for choice in enum]
            if bad := [c for c, value in zip(enum, choices) if not _is_field_type(field, value)]:
                raise ValueError(
                    f"{where} looks up param {source.name!r}, whose choices {bad} are not valid here"
                )
        elif (
            self.translation is None
            and LOOKUP_FIELDS[field] is int
            and "minimum" in schema
            and "maximum" in schema
        ):
            if field == "pool_slots" and schema["minimum"] < 1:
                raise ValueError(f"{where} looks up param {source.name!r}, whose minimum is below 1")
        else:
            bound = "enum, or minimum and maximum," if LOOKUP_FIELDS[field] is int else "enum"
            raise ValueError(
                f"{where} looks up param {source.name!r}, which must declare {bound} "
                "so that whoever triggers a run chooses among values the Dag author listed"
            )


def reject_lookups_in_partial(partial_kwargs: Mapping[str, Any], task_id: str) -> None:
    """Lookups on mapped tasks are not part of this proof of concept; overrides still reach them."""
    if fields := sorted(k for k in LOOKUP_FIELDS if isinstance(partial_kwargs.get(k), Lookup)):
        raise ValueError(
            f"Task {task_id!r} uses a Lookup for {fields} in partial(), which is not supported yet"
        )
