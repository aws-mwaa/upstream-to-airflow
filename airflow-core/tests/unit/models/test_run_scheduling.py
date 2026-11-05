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

from unittest import mock

import pytest
from sqlalchemy import select

from airflow import settings
from airflow.models.pool import Pool
from airflow.models.run_scheduling import SchedulingValueRejected
from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Lookup, Param, TaskGroup
from airflow.settings import get_policy_plugin_manager
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_pools, clear_db_runs
from tests_common.test_utils.mock_operators import MockOperator

pytestmark = pytest.mark.db_test

POOL_PARAMS = {"warehouse_pool": Param("wh_small", enum=["wh_small", "wh_large"])}


@pytest.fixture(autouse=True)
def _clean_db():
    clear_db_runs()
    clear_db_dags()
    clear_db_pools()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_pools()


@pytest.fixture
def pools(session):
    for name, slots, allow in [
        ("wh_small", 2, False),
        ("wh_large", 16, False),
        ("heavy", 4, True),
        ("closed", 8, False),
        ("overflow", 32, True),
    ]:
        session.add(Pool(pool=name, slots=slots, include_deferred=False, allow_overrides=allow))
    session.commit()


def _ti_values(session, dag_run, task_id: str) -> dict:
    ti = session.scalar(select(TI).where(TI.run_id == dag_run.run_id, TI.task_id == task_id))
    return {f: getattr(ti, f) for f in ("pool", "pool_slots", "priority_weight", "queue")}


@pytest.fixture(params=[True, False], ids=["bulk-insert", "mutation-hook"])
def hook_is_noop(request, monkeypatch):
    """Run each test through both task instance creation paths."""
    monkeypatch.setattr(settings.task_instance_mutation_hook, "is_noop", request.param)
    return request.param


@pytest.mark.usefixtures("pools", "hook_is_noop")
class TestLookups:
    def test_param_lookup_follows_conf(self, dag_maker, session):
        with dag_maker("d", params=POOL_PARAMS, session=session):
            EmptyOperator(task_id="merge", pool=Lookup.param("warehouse_pool"))
        dr = dag_maker.create_dagrun(conf={"warehouse_pool": "wh_large"})
        assert _ti_values(session, dr, "merge")["pool"] == "wh_large"

    def test_param_lookup_uses_default_without_conf(self, dag_maker, session):
        with dag_maker("d", params=POOL_PARAMS, session=session):
            EmptyOperator(task_id="merge", pool=Lookup.param("warehouse_pool"))
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        assert _ti_values(session, dr, "merge")["pool"] == "wh_small"

    @pytest.mark.parametrize(
        ("run_type", "expected"),
        [(DagRunType.BACKFILL_JOB, "wh_large"), (DagRunType.MANUAL, "wh_small")],
    )
    def test_run_type_lookup(self, dag_maker, session, run_type, expected):
        with dag_maker("d", session=session):
            EmptyOperator(task_id="load", pool=Lookup.run_type({"backfill": "wh_large"}, default="wh_small"))
        dr = dag_maker.create_dagrun(run_type=run_type)
        assert _ti_values(session, dr, "load")["pool"] == expected

    def test_map_and_priority_weight_is_absolute(self, dag_maker, session):
        params = {"size": Param("small", enum=["small", "large"])}
        with dag_maker("d", params=params, session=session):
            first = EmptyOperator(
                task_id="first",
                pool_slots=Lookup.param("size").map({"small": 1, "large": 4}),
                priority_weight=Lookup.param("size").map({"small": 5, "large": 50}),
            )
            first >> EmptyOperator(task_id="second")
        dr = dag_maker.create_dagrun(conf={"size": "large"})
        # weight_rule=downstream would add the downstream task's weight; a looked-up weight replaces it.
        assert _ti_values(session, dr, "first") | {"pool": None} == {
            "pool": None,
            "pool_slots": 4,
            "priority_weight": 50,
            "queue": "default",
        }

    def test_request_with_unknown_lookup_pool_is_rejected(self, dag_maker, session):
        params = {"p": Param("wh_small", enum=["wh_small", "nowhere"])}
        with dag_maker("d", params=params, session=session):
            EmptyOperator(task_id="t", pool=Lookup.param("p"))
        with pytest.raises(SchedulingValueRejected, match="pool='nowhere' cannot be used"):
            dag_maker.create_dagrun(conf={"p": "nowhere"})

    def test_scheduled_run_with_unknown_lookup_pool_is_not_rejected(self, dag_maker, session):
        with dag_maker("d", session=session):
            EmptyOperator(task_id="t", pool=Lookup.run_type({"scheduled": "nowhere"}, default="wh_small"))
        dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

    def test_request_with_pool_slots_above_pool_is_rejected(self, dag_maker, session):
        params = {"n": Param(1, type="integer", minimum=1, maximum=8)}
        with dag_maker("d", params=params, session=session):
            EmptyOperator(task_id="t", pool="wh_small", pool_slots=Lookup.param("n"))
        with pytest.raises(SchedulingValueRejected, match="pool_slots=3"):
            dag_maker.create_dagrun(conf={"n": 3})


@pytest.mark.usefixtures("pools", "hook_is_noop")
class TestOverrides:
    def test_task_entry_wins_over_group_entry(self, dag_maker, session):
        with dag_maker("d", session=session):
            with TaskGroup("transform"):
                EmptyOperator(task_id="a", pool="wh_small")
                EmptyOperator(task_id="b", pool="wh_small")
        dr = dag_maker.create_dagrun(
            scheduling_overrides={
                "tasks": {
                    "transform": {"pool": "heavy", "pool_slots": 2},
                    "transform.b": {"pool": "overflow"},
                }
            }
        )
        assert _ti_values(session, dr, "transform.a")["pool"] == "heavy"
        assert _ti_values(session, dr, "transform.a")["pool_slots"] == 2
        assert _ti_values(session, dr, "transform.b")["pool"] == "overflow"
        # The group's pool_slots still applies, against the task's own pool.
        assert _ti_values(session, dr, "transform.b")["pool_slots"] == 2

    def test_pools_remap_moves_lookup_and_declared_pools(self, dag_maker, session):
        with dag_maker("d", params=POOL_PARAMS, session=session):
            EmptyOperator(task_id="declared", pool="wh_small")
            EmptyOperator(task_id="looked_up", pool=Lookup.param("warehouse_pool"))
            EmptyOperator(task_id="other", pool="wh_large")
        dr = dag_maker.create_dagrun(scheduling_overrides={"pools": {"wh_small": "overflow"}})
        assert _ti_values(session, dr, "declared")["pool"] == "overflow"
        assert _ti_values(session, dr, "looked_up")["pool"] == "overflow"
        assert _ti_values(session, dr, "other")["pool"] == "wh_large"

    @pytest.mark.parametrize(
        "overrides",
        [
            pytest.param({"tasks": {"t": {"pool": "closed"}}}, id="pool-not-nominated"),
            pytest.param({"tasks": {"t": {"pool": "nowhere"}}}, id="pool-unknown"),
            pytest.param({"tasks": {"t": {"pool_slots": 2}}}, id="slots-in-unnominated-pool"),
            pytest.param({"tasks": {"t": {"pool": "heavy", "pool_slots": 5}}}, id="slots-above-pool"),
            pytest.param({"tasks": {"t": {"queue": "gpu"}}}, id="queue-not-listed"),
        ],
    )
    def test_unusable_override_rejected(self, dag_maker, session, overrides):
        with dag_maker("d", session=session):
            EmptyOperator(task_id="t", pool="wh_small")
        with pytest.raises(SchedulingValueRejected, match="cannot be used for task 't' in this run"):
            dag_maker.create_dagrun(scheduling_overrides=overrides)

    def test_unknown_and_unnominated_pools_get_same_error(self, dag_maker, session):
        with dag_maker("d", session=session):
            EmptyOperator(task_id="t")
        messages = []
        for pool in ("closed", "nowhere"):
            with pytest.raises(SchedulingValueRejected) as ctx:
                dag_maker.create_dagrun(run_id=pool, scheduling_overrides={"tasks": {"t": {"pool": pool}}})
            messages.append(str(ctx.value).replace(pool, "<pool>"))
        assert messages[0] == messages[1]

    @conf_vars({("core", "override_target_queues"): "gpu, highmem"})
    def test_listed_queue_accepted(self, dag_maker, session):
        with dag_maker("d", session=session):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun(scheduling_overrides={"tasks": {"t": {"queue": "gpu"}}})
        assert _ti_values(session, dr, "t")["queue"] == "gpu"

    def test_unknown_task_key_rejected(self, dag_maker, session):
        with dag_maker("d", session=session):
            EmptyOperator(task_id="t")
        with pytest.raises(SchedulingValueRejected, match="unknown tasks or groups"):
            dag_maker.create_dagrun(scheduling_overrides={"tasks": {"nope": {"pool": "heavy"}}})

    def test_scheduled_run_cannot_carry_overrides(self, dag_maker, session):
        with dag_maker("d", session=session):
            EmptyOperator(task_id="t")
        with pytest.raises(SchedulingValueRejected, match="only be set on runs triggered"):
            dag_maker.create_dagrun(
                run_type=DagRunType.SCHEDULED, scheduling_overrides={"tasks": {"t": {"pool": "heavy"}}}
            )

    def test_unnominating_falls_back_on_clear(self, dag_maker, session):
        with dag_maker("d", session=session):
            EmptyOperator(task_id="t", pool="wh_small")
        dr = dag_maker.create_dagrun(
            scheduling_overrides={"tasks": {"t": {"pool": "heavy", "pool_slots": 3}}}
        )
        assert _ti_values(session, dr, "t")["pool"] == "heavy"

        session.execute(Pool.__table__.update().where(Pool.pool == "heavy").values(allow_overrides=False))
        session.commit()
        session.expunge_all()
        tis = session.scalars(select(TI).where(TI.run_id == dr.run_id)).all()
        clear_task_instances(tis, session)
        session.commit()

        assert _ti_values(session, dr, "t") | {"priority_weight": None} == {
            "pool": "wh_small",
            "pool_slots": 1,
            "priority_weight": None,
            "queue": "default",
        }

    def test_mapped_task_instances_get_override(self, dag_maker, session):
        with dag_maker("d", session=session):
            MockOperator.partial(task_id="mapped").expand(arg2=[1, 2, 3])
        dr = dag_maker.create_dagrun(scheduling_overrides={"tasks": {"mapped": {"pool": "heavy"}}})
        dr.task_instance_scheduling_decisions(session=session)
        session.flush()
        pools = session.scalars(select(TI.pool).where(TI.run_id == dr.run_id, TI.task_id == "mapped")).all()
        assert len(pools) == 3
        assert set(pools) == {"heavy"}


@pytest.mark.usefixtures("pools")
def test_cluster_policy_runs_after_overrides(dag_maker, session):
    def mutate(task_instance, dag_run=None):
        task_instance.pool = "wh_large"

    with mock.patch.object(
        get_policy_plugin_manager().hook, "task_instance_mutation_hook", autospec=True
    ) as hook:
        hook.side_effect = mutate
        with dag_maker("d", session=session):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun(scheduling_overrides={"tasks": {"t": {"pool": "heavy"}}})
    assert _ti_values(session, dr, "t")["pool"] == "wh_large"


def test_no_resolver_for_plain_runs(dag_maker, session):
    with dag_maker("d", session=session):
        EmptyOperator(task_id="t")
    with mock.patch("airflow.models.dagrun.DagRun._build_scheduling_resolver", autospec=True) as build:
        dag_maker.create_dagrun()
    build.assert_not_called()
