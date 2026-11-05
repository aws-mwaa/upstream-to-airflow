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

from airflow.sdk import DAG, BaseOperator, Lookup, Param, TaskGroup, task

POOL_PARAMS = {"warehouse_pool": Param("wh_small", enum=["wh_small", "wh_large"])}


class TestLookupConstruction:
    def test_run_type_rejects_unknown_run_type(self):
        with pytest.raises(ValueError, match="unknown run types"):
            Lookup.run_type({"nightly": "p"}, default="etl_pool")

    def test_param_needs_name(self):
        with pytest.raises(ValueError, match="needs a param name"):
            Lookup.param("")


class TestParseTimeValue:
    def test_param_default_is_plain_value(self):
        with DAG("d", schedule=None, params=POOL_PARAMS):
            op = BaseOperator(task_id="t", pool=Lookup.param("warehouse_pool"))
        assert op.pool == "wh_small"
        assert op.scheduling_lookups == {"pool": Lookup.param("warehouse_pool")}

    def test_explicit_default_wins(self):
        with DAG("d", schedule=None):
            op = BaseOperator(task_id="t", pool=Lookup.run_type({"backfill": "bf"}, default="etl_pool"))
        assert op.pool == "etl_pool"

    def test_map_translates_param_default(self):
        params = {"size": Param("small", enum=["small", "large"])}
        with DAG("d", schedule=None, params=params):
            op = BaseOperator(task_id="t", pool_slots=Lookup.param("size").map({"small": 1, "large": 4}))
        assert op.pool_slots == 1

    def test_task_created_outside_dag_is_synced_in_validate(self):
        op = BaseOperator(task_id="t", pool=Lookup.param("warehouse_pool"))
        assert op.pool == "default_pool"
        dag = DAG("d", schedule=None, params=POOL_PARAMS)
        dag.add_task(op)
        dag.validate()
        assert op.pool == "wh_small"

    def test_dag_and_task_group_default_args(self):
        with DAG(
            "d", schedule=None, params=POOL_PARAMS, default_args={"pool": Lookup.param("warehouse_pool")}
        ):
            outer = BaseOperator(task_id="outer")
            with TaskGroup("g", default_args={"priority_weight": Lookup.run_type({"manual": 9}, default=1)}):
                inner = BaseOperator(task_id="inner")
        assert set(outer.scheduling_lookups) == {"pool"}
        assert set(inner.scheduling_lookups) == {"pool", "priority_weight"}
        assert inner.priority_weight == 1

    def test_decorated_task(self):
        with DAG("d", schedule=None, params=POOL_PARAMS) as dag:

            @task(pool=Lookup.param("warehouse_pool"))
            def work():
                pass

            work()
        assert dag.get_task("work").scheduling_lookups == {"pool": Lookup.param("warehouse_pool")}


class TestValidation:
    @pytest.mark.parametrize(
        ("params", "field", "lookup", "match"),
        [
            pytest.param({}, "pool", Lookup.param("missing"), "does not declare", id="missing-param"),
            pytest.param(
                {"p": Param("a", type="string")},
                "pool",
                Lookup.param("p"),
                "must declare enum",
                id="unbounded",
            ),
            pytest.param(
                {"n": Param(2, type="integer", minimum=1)},
                "pool_slots",
                Lookup.param("n"),
                "enum, or minimum and maximum",
                id="half-bounded-int",
            ),
            pytest.param(
                {"n": Param(1, type="integer", minimum=0, maximum=4)},
                "pool_slots",
                Lookup.param("n"),
                "minimum is below 1",
                id="zero-slots",
            ),
            pytest.param(
                {"size": Param("small", enum=["small", "large"])},
                "pool_slots",
                Lookup.param("size").map({"small": 1}),
                "are not valid here",
                id="unmapped-choice",
            ),
            pytest.param(
                {"n": Param(1, enum=[1, 2])}, "pool", Lookup.param("n"), "are not valid here", id="wrong-type"
            ),
        ],
    )
    def test_rejected(self, params, field, lookup, match):
        with DAG("d", schedule=None, params=params) as dag:
            BaseOperator(task_id="t", **{field: lookup})
        with pytest.raises(ValueError, match=match):
            dag.validate()

    def test_run_type_without_default_rejected(self):
        with DAG("d", schedule=None) as dag:
            op = BaseOperator(task_id="t")
        op.scheduling_lookups["pool"] = Lookup(
            sources=Lookup.run_type({"backfill": "bf"}, default="x").sources
        )
        with pytest.raises(ValueError, match="without default="):
            dag.validate()

    @pytest.mark.parametrize(
        ("params", "field", "lookup"),
        [
            pytest.param(POOL_PARAMS, "pool", Lookup.param("warehouse_pool"), id="enum"),
            pytest.param(
                {"n": Param(2, type="integer", minimum=1, maximum=8)},
                "pool_slots",
                Lookup.param("n"),
                id="int-range",
            ),
            pytest.param(
                {}, "queue", Lookup.run_type({"backfill": "slow"}, default="default"), id="run-type"
            ),
        ],
    )
    def test_accepted(self, params, field, lookup):
        with DAG("d", schedule=None, params=params) as dag:
            BaseOperator(task_id="t", **{field: lookup})
        dag.validate()

    def test_partial_rejected(self):
        with DAG("d", schedule=None, params=POOL_PARAMS):
            with pytest.raises(ValueError, match="not supported yet"):
                BaseOperator.partial(task_id="t", pool=Lookup.param("warehouse_pool")).expand(retries=[1, 2])
