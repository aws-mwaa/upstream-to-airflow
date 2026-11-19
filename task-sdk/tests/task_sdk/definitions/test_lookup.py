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

from airflow.sdk import DAG, BaseOperator, Param, TaskGroup, by_run_type, dag as dag_decorator, task
from airflow.sdk.definitions.param import DagParam

PARAM_LOOKUP = {"sources": [{"kind": "param", "name": "warehouse_pool"}]}


def pool_params() -> dict[str, Param]:
    # A new Param each time: dag.param(name, default) changes the Param it is given.
    return {"warehouse_pool": Param("wh_small", enum=["wh_small", "wh_large"])}


class FanOperator(BaseOperator):
    template_fields = ("x",)

    def __init__(self, x=None, **kwargs):
        super().__init__(**kwargs)
        self.x = x


def _serialize_lookups(operator) -> dict:
    return {field: lookup.serialize() for field, lookup in operator.scheduling_lookups.items()}


class TestByRunType:
    def test_serializes_run_types_and_default(self):
        lookup = by_run_type(backfill="backfill_pool", asset_triggered="asset_pool", default="etl_pool")
        assert lookup.serialize() == {
            "sources": [
                {
                    "kind": "run_type",
                    "mapping": {"backfill": "backfill_pool", "asset_triggered": "asset_pool"},
                }
            ],
            "default": "etl_pool",
        }

    def test_unknown_run_type_rejected(self):
        with pytest.raises(ValueError, match="unknown run types \\['nightly'\\]"):
            by_run_type(nightly="p", default="etl_pool")

    def test_default_is_required(self):
        with pytest.raises(TypeError, match="default"):
            by_run_type(backfill="backfill_pool")

    def test_needs_a_run_type(self):
        with pytest.raises(ValueError, match="at least one run type"):
            by_run_type(default="etl_pool")

    @pytest.mark.parametrize(
        ("field", "lookup"),
        [
            pytest.param("pool_slots", by_run_type(backfill="many", default=1), id="value"),
            pytest.param("pool_slots", by_run_type(backfill=2, default=0), id="default-below-one-slot"),
            pytest.param("queue", by_run_type(backfill="slow", default=None), id="default"),
            pytest.param("priority_weight", by_run_type(manual=True, default=1), id="bool"),
        ],
    )
    def test_value_of_wrong_type_rejected(self, field, lookup):
        with pytest.raises(ValueError, match="which is not valid here"):
            BaseOperator(task_id="t", **{field: lookup})


class TestParseTimeValue:
    def test_dag_param_field_holds_param_default(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            op = BaseOperator(task_id="t", pool=dag.param("warehouse_pool"))
        dag.validate()
        assert op.pool == "wh_small"
        assert dag.params["warehouse_pool"] == "wh_small"
        assert _serialize_lookups(op) == {"pool": PARAM_LOOKUP}

    def test_by_run_type_field_holds_default(self):
        with DAG("d", schedule=None) as dag:
            op = BaseOperator(task_id="t", queue=by_run_type(backfill="slow", default="fast"))
        dag.validate()
        assert op.queue == "fast"

    def test_task_created_before_it_joins_the_dag(self):
        dag = DAG("d", schedule=None, params=pool_params())
        op = BaseOperator(task_id="t", pool=dag.param("warehouse_pool"))
        assert op.pool == "wh_small"
        dag.add_task(op)
        dag.validate()
        assert op.pool == "wh_small"

    def test_assigned_after_the_task_was_created(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            op = BaseOperator(task_id="t")
            op.pool = dag.param("warehouse_pool")
        dag.validate()
        assert op.pool == "wh_small"
        assert _serialize_lookups(op) == {"pool": PARAM_LOOKUP}

    def test_dag_default_args_set_inside_with_block(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            before = BaseOperator(task_id="before")
            dag.default_args["pool"] = dag.param("warehouse_pool")
            after = BaseOperator(task_id="after")
        dag.validate()
        assert before.pool == "default_pool"
        assert before.scheduling_lookups == {}
        assert after.pool == "wh_small"
        assert _serialize_lookups(after) == {"pool": PARAM_LOOKUP}

    def test_task_group_default_args(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            with TaskGroup("given", default_args={"pool": dag.param("warehouse_pool")}) as given:
                in_given = BaseOperator(task_id="t")
            with TaskGroup("mutated") as mutated:
                before = BaseOperator(task_id="before")
                mutated.default_args["priority_weight"] = by_run_type(manual=9, default=2)
                after = BaseOperator(task_id="after")
        dag.validate()
        # The group deep-copies its default_args; copying the DagParam would copy the whole Dag.
        assert given.default_args["pool"].current_dag is dag
        assert in_given.pool == "wh_small"
        assert _serialize_lookups(in_given) == {"pool": PARAM_LOOKUP}
        assert before.scheduling_lookups == {}
        assert after.priority_weight == 2
        assert set(after.scheduling_lookups) == {"priority_weight"}

    def test_decorated_task(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:

            @task(pool=dag.param("warehouse_pool"))
            def work():
                pass

            work()
        dag.validate()
        assert dag.get_task("work").pool == "wh_small"
        assert _serialize_lookups(dag.get_task("work")) == {"pool": PARAM_LOOKUP}

    def test_dag_decorator_argument(self):
        @dag_decorator(schedule=None, params=pool_params())
        def regional_sync(warehouse_pool="wh_large"):
            BaseOperator(task_id="t", pool=warehouse_pool)

        dag = regional_sync()
        dag.validate()
        # The function's default replaces the one in params.
        assert dag.get_task("t").pool == "wh_large"
        assert dag.params["warehouse_pool"] == "wh_large"

    def test_templated_field_keeps_dag_param(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            op = FanOperator(task_id="t", x=dag.param("warehouse_pool"))
        dag.validate()
        assert isinstance(op.x, DagParam)
        assert op.scheduling_lookups == {}


class TestPartial:
    def test_partial_holds_plain_value_and_lookup(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            fan = FanOperator.partial(
                task_id="fan",
                pool=dag.param("warehouse_pool"),
                pool_slots=by_run_type(backfill=2, default=1),
            ).expand(x=[1, 2, 3])
        dag.validate()
        assert fan.pool == "wh_small"
        assert fan.partial_kwargs["pool"] == "wh_small"
        assert fan.partial_kwargs["pool_slots"] == 1
        assert set(fan.scheduling_lookups) == {"pool", "pool_slots"}
        assert fan.scheduling_lookups["pool"].serialize() == PARAM_LOOKUP

    def test_partial_takes_dag_default_args(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            dag.default_args["pool"] = dag.param("warehouse_pool")
            fan = FanOperator.partial(task_id="fan").expand(x=[1, 2, 3])
        dag.validate()
        assert fan.pool == "wh_small"
        assert _serialize_lookups(fan) == {"pool": PARAM_LOOKUP}

    def test_decorated_task_expand(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:

            @task(pool=dag.param("warehouse_pool"))
            def work(x):
                pass

            work.expand(x=[1, 2, 3])
        dag.validate()
        assert dag.get_task("work").partial_kwargs["pool"] == "wh_small"
        assert _serialize_lookups(dag.get_task("work")) == {"pool": PARAM_LOOKUP}

    def test_unmapped_task_gets_plain_value(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            fan = FanOperator.partial(task_id="fan", pool=dag.param("warehouse_pool")).expand(x=[1, 2])
        dag.validate()
        assert fan.unmap({"x": 1}).pool == "wh_small"


class TestDagParamDefault:
    def test_default_inside_enum_accepted(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            op = BaseOperator(task_id="t", pool=dag.param("warehouse_pool", "wh_large"))
        dag.validate()
        assert op.pool == "wh_large"
        assert dag.params["warehouse_pool"] == "wh_large"

    def test_undeclared_param_rejected(self):
        with DAG("d", schedule=None) as dag:
            BaseOperator(task_id="t", pool=dag.param("free_text", "anything"))
        with pytest.raises(ValueError, match="must declare enum or const"):
            dag.validate()

    def test_default_replaced_by_a_later_call_rejected(self):
        with DAG("d", schedule=None, params=pool_params()) as dag:
            BaseOperator(task_id="first", pool=dag.param("warehouse_pool", "wh_small"))
            BaseOperator(task_id="second", pool=dag.param("warehouse_pool", "wh_large"))
        with pytest.raises(ValueError, match="task 'first'.*default 'wh_small'.*is now 'wh_large'"):
            dag.validate()


class TestValidation:
    @pytest.mark.parametrize(
        ("params", "field", "name", "match"),
        [
            pytest.param({}, "pool", "missing", "does not declare in params", id="missing-param"),
            pytest.param(
                {"p": Param("a", type="string")}, "pool", "p", "must declare enum or const", id="unbounded"
            ),
            pytest.param(
                {"n": Param(2, type="integer", minimum=1)},
                "pool_slots",
                "n",
                "enum, or type 'integer' with minimum and maximum",
                id="half-bounded-int",
            ),
            pytest.param(
                {"n": Param(2, type="number", minimum=1, maximum=8)},
                "pool_slots",
                "n",
                "enum, or type 'integer' with minimum and maximum",
                id="range-of-numbers",
            ),
            pytest.param(
                {"n": Param(2, type=["integer", "null"], minimum=1, maximum=8)},
                "priority_weight",
                "n",
                "enum, or type 'integer' with minimum and maximum",
                id="nullable-int",
            ),
            pytest.param(
                {"n": Param(2, const=2)},
                "priority_weight",
                "n",
                "enum, or type 'integer' with minimum and maximum",
                id="int-const",
            ),
            pytest.param(
                {"n": Param(1, type="integer", minimum=0, maximum=4)},
                "pool_slots",
                "n",
                "minimum is below 1",
                id="zero-slots-in-range",
            ),
            pytest.param(
                {"n": Param(1, enum=[0, 1])}, "pool_slots", "n", "choices \\[0\\]", id="zero-slots-in-enum"
            ),
            pytest.param({"n": Param(1, enum=[1, 2])}, "pool", "n", "choices \\[1, 2\\]", id="wrong-type"),
            pytest.param(
                {"p": Param("a", enum=["a", None])}, "queue", "p", "choices \\[None\\]", id="nullable-enum"
            ),
        ],
    )
    def test_rejected(self, params, field, name, match):
        with DAG("d", schedule=None, params=params) as dag:
            BaseOperator(task_id="t", **{field: dag.param(name)})
        with pytest.raises(ValueError, match=match):
            dag.validate()

    @pytest.mark.parametrize(
        ("params", "field", "name", "expected"),
        [
            pytest.param(pool_params(), "pool", "warehouse_pool", "wh_small", id="enum"),
            pytest.param({"q": Param("gpu", const="gpu")}, "queue", "q", "gpu", id="const"),
            pytest.param({"n": Param(2, enum=[1, 2, 4])}, "pool_slots", "n", 2, id="int-enum"),
            pytest.param(
                {"n": Param(5, type="integer", minimum=1, maximum=100)},
                "priority_weight",
                "n",
                5,
                id="int-range",
            ),
        ],
    )
    def test_accepted(self, params, field, name, expected):
        with DAG("d", schedule=None, params=params) as dag:
            op = BaseOperator(task_id="t", **{field: dag.param(name)})
        dag.validate()
        assert getattr(op, field) == expected

    def test_dag_param_of_another_dag_rejected(self):
        other = DAG("other", schedule=None, params=pool_params())
        with DAG("d", schedule=None, params=pool_params()) as dag:
            BaseOperator(task_id="t", pool=other.param("warehouse_pool"))
        with pytest.raises(ValueError, match="of Dag 'other', but the task is in Dag 'd'"):
            dag.validate()
