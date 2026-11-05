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
from __future__ import annotations

import pytest

from airflow.sdk import DAG, BaseOperator, Lookup, Param
from airflow.serialization.definitions.lookup import decode_lookup
from airflow.serialization.definitions.notset import NOTSET
from airflow.serialization.enums import Encoding
from airflow.serialization.serialized_objects import DagSerialization

PARAMS = {"size": Param("small", enum=["small", "large"])}


def _serialize(**task_kwargs) -> dict:
    with DAG("d", schedule=None, params=PARAMS) as dag:
        BaseOperator(task_id="t", **task_kwargs)
    return DagSerialization.to_dict(dag)


def test_round_trip():
    ser = _serialize(
        pool=Lookup.run_type({"backfill": "bf"}, default="etl"),
        pool_slots=Lookup.param("size").map({"small": 1, "large": 4}),
    )
    encoded = ser["dag"]["tasks"][0][Encoding.VAR]
    assert encoded["scheduling_lookups"] == {
        "pool": {"sources": [{"kind": "run_type", "mapping": {"backfill": "bf"}}], "default": "etl"},
        "pool_slots": {"sources": [{"kind": "param", "name": "size"}], "map": [["small", 1], ["large", 4]]},
    }
    # The plain field holds the value a run without conf gets.
    assert encoded["pool"] == "etl"

    task = DagSerialization.from_dict(ser).task_dict["t"]
    assert task.pool == "etl"
    lookups = task.scheduling_lookups
    assert lookups["pool"].resolve("pool", params={}, run_type="backfill") == "bf"
    assert lookups["pool"].resolve("pool", params={}, run_type="manual") == "etl"
    assert lookups["pool_slots"].resolve("pool_slots", params={"size": "large"}, run_type="manual") == 4


def test_task_without_lookups_serializes_as_before():
    encoded = _serialize(pool="p")["dag"]["tasks"][0][Encoding.VAR]
    assert "scheduling_lookups" not in encoded


def test_decode_rejects_unknown_source_kind():
    with pytest.raises(ValueError, match="Unknown Lookup source 'class'"):
        decode_lookup({"sources": [{"kind": "class", "name": "os.system"}]})


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        pytest.param("pool_slots", True, NOTSET, id="bool-is-not-int"),
        pytest.param("pool_slots", 0, NOTSET, id="zero-slots"),
        pytest.param("pool", "", NOTSET, id="empty-pool"),
        pytest.param("priority_weight", 2**40, 2147483647, id="weight-clamped"),
    ],
)
def test_resolve_coerces_values(field, value, expected):
    lookup = decode_lookup({"sources": [{"kind": "param", "name": "x"}]})
    assert lookup.resolve(field, params={"x": value}, run_type="manual") == expected
