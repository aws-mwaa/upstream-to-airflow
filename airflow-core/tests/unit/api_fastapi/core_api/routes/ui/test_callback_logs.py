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
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.state import CallbackState, DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_deadline,
    clear_db_deadline_alert,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

DAG_ID = "test_callback_logs_dag"
RUN_ID = "test_callback_logs_run"

_CALLBACK_PATH = "tests.unit.api_fastapi.core_api.routes.ui.test_callback_logs._noop_callback"


async def _noop_callback(**kwargs):
    """No-op callback used to satisfy Deadline creation in tests."""


def _cb() -> AsyncCallback:
    return AsyncCallback(_CALLBACK_PATH)


@pytest.fixture(autouse=True)
def setup(dag_maker, session):
    clear_db_deadline()
    clear_db_deadline_alert()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()

    with dag_maker(DAG_ID, serialized=True, session=session):
        EmptyOperator(task_id="task")

    dag_maker.create_dagrun(
        run_id=RUN_ID,
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2024, 11, 1),
        triggered_by=DagRunTriggeredByType.TEST,
    )

    dag_maker.sync_dagbag_to_db()
    session.commit()
    yield
    clear_db_deadline()
    clear_db_deadline_alert()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


class TestGetCallbackLogs:
    """Tests for GET /dags/{dag_id}/dagRuns/{dag_run_id}/callbacks/{callback_id}/logs."""

    def test_callback_not_found_returns_404(self, test_client):
        fake_id = str(uuid7())
        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/callbacks/{fake_id}/logs")
        assert response.status_code == 404
        assert "was not found" in response.json()["detail"]

    def test_dag_run_not_found_returns_404(self, test_client, session):
        from airflow.models.dagrun import DagRun

        dag_run = session.scalar(
            DagRun.__table__.select().where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID)
        )

        dl = Deadline(
            deadline_time=timezone.datetime(2025, 1, 1, 12, 0, 0),
            callback=_cb(),
            dagrun_id=dag_run.id,
            deadline_alert_id=None,
        )
        session.add(dl)
        session.flush()
        callback_id = str(dl.callback_id)
        session.commit()

        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/nonexistent_run/callbacks/{callback_id}/logs")
        assert response.status_code == 404
        assert "was not found" in response.json()["detail"]

    @mock.patch(
        "airflow.api_fastapi.core_api.routes.ui.deadlines.read_callback_log",
        autospec=True,
    )
    def test_returns_logs_json(self, mock_read_log, test_client, session):
        from airflow.models.dagrun import DagRun
        from airflow.utils.log.file_task_handler import StructuredLogMessage

        dag_run = session.scalar(
            DagRun.__table__.select().where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID)
        )

        dl = Deadline(
            deadline_time=timezone.datetime(2025, 1, 1, 12, 0, 0),
            callback=_cb(),
            dagrun_id=dag_run.id,
            deadline_alert_id=None,
        )
        session.add(dl)
        session.flush()
        callback_id = str(dl.callback_id)
        session.commit()

        mock_read_log.return_value = iter(
            [
                StructuredLogMessage(event="Callback started", timestamp=None),
                StructuredLogMessage(event="Callback completed", timestamp=None),
            ]
        )

        response = test_client.get(
            f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/callbacks/{callback_id}/logs",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["continuation_token"] is None
        assert len(data["content"]) == 2
        assert data["content"][0]["event"] == "Callback started"
        assert data["content"][1]["event"] == "Callback completed"

        mock_read_log.assert_called_once_with(
            dag_id=DAG_ID,
            run_id=RUN_ID,
            callback_id=callback_id,
        )

    @mock.patch(
        "airflow.api_fastapi.core_api.routes.ui.deadlines.read_callback_log",
        autospec=True,
    )
    def test_returns_logs_ndjson_stream(self, mock_read_log, test_client, session):
        from airflow.models.dagrun import DagRun
        from airflow.utils.log.file_task_handler import StructuredLogMessage

        dag_run = session.scalar(
            DagRun.__table__.select().where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID)
        )

        dl = Deadline(
            deadline_time=timezone.datetime(2025, 1, 1, 12, 0, 0),
            callback=_cb(),
            dagrun_id=dag_run.id,
            deadline_alert_id=None,
        )
        session.add(dl)
        session.flush()
        callback_id = str(dl.callback_id)
        session.commit()

        mock_read_log.return_value = iter(
            [
                StructuredLogMessage(event="Log line 1", timestamp=None),
                StructuredLogMessage(event="Log line 2", timestamp=None),
            ]
        )

        response = test_client.get(
            f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/callbacks/{callback_id}/logs",
            headers={"Accept": "application/x-ndjson"},
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-ndjson"
        lines = response.text.strip().split("\n")
        assert len(lines) == 2

    @mock.patch(
        "airflow.api_fastapi.core_api.routes.ui.deadlines.read_callback_log",
        autospec=True,
    )
    def test_no_logs_found(self, mock_read_log, test_client, session):
        from airflow.models.dagrun import DagRun
        from airflow.utils.log.file_task_handler import StructuredLogMessage

        dag_run = session.scalar(
            DagRun.__table__.select().where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID)
        )

        dl = Deadline(
            deadline_time=timezone.datetime(2025, 1, 1, 12, 0, 0),
            callback=_cb(),
            dagrun_id=dag_run.id,
            deadline_alert_id=None,
        )
        session.add(dl)
        session.flush()
        callback_id = str(dl.callback_id)
        session.commit()

        mock_read_log.return_value = iter(
            [StructuredLogMessage(event="No callback logs found.", timestamp=None)]
        )

        response = test_client.get(
            f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/callbacks/{callback_id}/logs",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["content"]) == 1
        assert "No callback logs found" in data["content"][0]["event"]


class TestDeadlineResponseIncludesCallbackInfo:
    """Tests that the deadline response includes callback_id and callback_state."""

    def test_deadline_response_includes_callback_fields(self, test_client, session):
        from airflow.models.dagrun import DagRun

        dag_run = session.scalar(
            DagRun.__table__.select().where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID)
        )

        dl = Deadline(
            deadline_time=timezone.datetime(2025, 1, 1, 12, 0, 0),
            callback=_cb(),
            dagrun_id=dag_run.id,
            deadline_alert_id=None,
        )
        session.add(dl)
        session.flush()
        callback_id = str(dl.callback_id)
        session.commit()

        response = test_client.get(f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/deadlines")
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 1
        deadline_data = data["deadlines"][0]
        assert deadline_data["callback_id"] == callback_id
        # Callback state should be "scheduled" (default for newly created callbacks)
        assert deadline_data["callback_state"] == CallbackState.SCHEDULED
