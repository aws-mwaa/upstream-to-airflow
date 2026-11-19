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

import json

import pytest

from airflow.sdk import DAG, BaseOperator, Param, by_run_type
from airflow.serialization.definitions.lookup import decode_lookup
from airflow.serialization.definitions.notset import NOTSET
from airflow.serialization.enums import Encoding
from airflow.serialization.serialized_objects import DagSerialization


def _make_dag() -> DAG:
    return DAG("d", schedule=None, params={"slots": Param(1, enum=[1, 4])})


def test_round_trip():
    with _make_dag() as dag:
        BaseOperator(
            task_id="t",
            pool=by_run_type(backfill="bf", default="etl"),
            pool_slots=dag.param("slots"),
        )
    ser = DagSerialization.to_dict(dag)
    encoded = ser["dag"]["tasks"][0][Encoding.VAR]
    assert encoded["scheduling_lookups"] == {
        "pool": {"sources": [{"kind": "run_type", "mapping": {"backfill": "bf"}}], "default": "etl"},
        "pool_slots": {"sources": [{"kind": "param", "name": "slots"}]},
    }
    # The plain field holds the value a run without conf gets.
    assert encoded["pool"] == "etl"

    task = DagSerialization.from_dict(ser).task_dict["t"]
    assert task.pool == "etl"
    lookups = task.scheduling_lookups
    assert lookups["pool"].resolve("pool", params={}, run_type="backfill") == "bf"
    assert lookups["pool"].resolve("pool", params={}, run_type="manual") == "etl"
    assert lookups["pool_slots"].resolve("pool_slots", params={"slots": 4}, run_type="manual") == 4


def test_mapped_operator_round_trip():
    with _make_dag() as dag:
        BaseOperator.partial(task_id="t", pool_slots=dag.param("slots")).expand(doc=["a", "b"])
    ser = DagSerialization.to_dict(dag)
    encoded = ser["dag"]["tasks"][0][Encoding.VAR]
    assert encoded["scheduling_lookups"] == {"pool_slots": {"sources": [{"kind": "param", "name": "slots"}]}}

    task = DagSerialization.from_dict(ser).task_dict["t"]
    assert task.is_mapped
    assert task.pool_slots == 1
    assert (
        task.scheduling_lookups["pool_slots"].resolve("pool_slots", params={"slots": 4}, run_type="manual")
        == 4
    )


def test_dag_default_args_store_plain_values():
    with _make_dag() as dag:
        dag.default_args["pool_slots"] = dag.param("slots")
        dag.default_args["queue"] = by_run_type(backfill="slow", default="fast")
        BaseOperator(task_id="t")
    ser = DagSerialization.to_dict(dag)
    assert ser["dag"]["default_args"][Encoding.VAR] == {"pool_slots": 1, "queue": "fast"}
    json.dumps(ser)


def test_task_without_lookups_serializes_as_before():
    with _make_dag() as dag:
        BaseOperator(task_id="t", pool="p")
        BaseOperator.partial(task_id="mapped", pool="p").expand(doc=["a", "b"])
    for task in DagSerialization.to_dict(dag)["dag"]["tasks"]:
        assert "scheduling_lookups" not in task[Encoding.VAR]


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
