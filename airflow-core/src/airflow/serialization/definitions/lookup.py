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
"""Server-side form of a task's scheduling Lookup (AIP-117)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import attrs

from airflow.serialization.definitions.notset import NOTSET, ArgNotSet

LOOKUP_FIELDS: dict[str, type] = {"pool": str, "pool_slots": int, "priority_weight": int, "queue": str}

# priority_weight is written to an INTEGER column.
_DB_SAFE_MINIMUM = -2147483648
_DB_SAFE_MAXIMUM = 2147483647


def coerce_field_value(field: str, value: Any) -> Any:
    """Return ``value`` if it is usable for ``field``, else ``NOTSET``."""
    if LOOKUP_FIELDS[field] is str:
        return value if isinstance(value, str) and value else NOTSET
    # bool is an int subclass: a boolean param would otherwise become one pool slot.
    if not isinstance(value, int) or isinstance(value, bool):
        return NOTSET
    if field == "pool_slots" and value < 1:
        return NOTSET
    if field == "priority_weight":
        return max(_DB_SAFE_MINIMUM, min(_DB_SAFE_MAXIMUM, value))
    return value


@attrs.frozen
class SerializedLookup:
    """
    A decoded Lookup the scheduler and API server can resolve without importing Dag code.

    ``sources`` holds ``("param", name)`` or ``("run_type", {run_type: value})`` pairs, tried in
    order. The encoded form is written by ``airflow.sdk.Lookup.serialize()``.
    """

    sources: tuple[tuple[str, Any], ...]
    default: Any = NOTSET
    translation: Mapping[Any, Any] | None = None

    @property
    def param_names(self) -> list[str]:
        return [arg for kind, arg in self.sources if kind == "param"]

    def resolve(self, field: str, *, params: Mapping[str, Any] | None, run_type: str) -> Any:
        """Return the run's value for ``field``, or ``NOTSET`` to keep the task's declared value."""
        value: Any = NOTSET
        for kind, arg in self.sources:
            if kind == "param":
                value = params.get(arg, NOTSET) if params is not None else NOTSET
            elif kind == "run_type":
                value = arg.get(run_type, NOTSET)
            if not isinstance(value, ArgNotSet):
                break
        if not isinstance(value, ArgNotSet) and self.translation is not None:
            try:
                value = self.translation.get(value, NOTSET)
            except TypeError:
                value = NOTSET
        if not isinstance(value, ArgNotSet):
            value = coerce_field_value(field, value)
        if isinstance(value, ArgNotSet) and not isinstance(self.default, ArgNotSet):
            value = coerce_field_value(field, self.default)
        return value


def decode_lookup(data: Mapping[str, Any]) -> SerializedLookup:
    """
    Decode through an allowlist of source kinds, so a serialized Dag cannot name a class to load.

    :raises ValueError: for an unknown source kind or a malformed entry.
    """
    sources: list[tuple[str, Any]] = []
    for source in data["sources"]:
        kind = source.get("kind")
        if kind == "param" and isinstance(source.get("name"), str):
            sources.append(("param", source["name"]))
        elif kind == "run_type" and isinstance(source.get("mapping"), dict):
            sources.append(("run_type", dict(source["mapping"])))
        else:
            raise ValueError(f"Unknown Lookup source {kind!r}")
    if not sources:
        raise ValueError("A Lookup needs at least one source")
    translation = None
    if (pairs := data.get("map")) is not None:
        translation = {key: value for key, value in pairs}
    return SerializedLookup(
        sources=tuple(sources),
        default=data.get("default", NOTSET),
        translation=translation,
    )
