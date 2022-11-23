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
from typing import Any
from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.dms import DmsHook, DmsTaskWaiterStatus

MOCK_DATA = {
    "replication_task_id": "test_task",
    "source_endpoint_arn": "source-endpoint-arn",
    "target_endpoint_arn": "target-endpoint-arn",
    "replication_instance_arn": "replication-instance-arn",
    "migration_type": "full-load",
    "table_mappings": {
        "rules": [
            {
                "rule-type": "selection",
                "rule-id": "1",
                "rule-name": "1",
                "object-locator": {
                    "schema-name": "test",
                    "table-name": "%",
                },
                "rule-action": "include",
            }
        ]
    },
}
MOCK_TASK_ARN = "task-arn"
MOCK_TASK_RESPONSE_DATA = {
    "ReplicationTaskIdentifier": MOCK_DATA["replication_task_id"],
    "SourceEndpointArn": MOCK_DATA["source_endpoint_arn"],
    "TargetEndpointArn": MOCK_DATA["target_endpoint_arn"],
    "ReplicationInstanceArn": MOCK_DATA["replication_instance_arn"],
    "MigrationType": MOCK_DATA["migration_type"],
    "TableMappings": json.dumps(MOCK_DATA["table_mappings"]),
    "ReplicationTaskArn": MOCK_TASK_ARN,
    "Status": "creating",
}
MOCK_DESCRIBE_RESPONSE: dict[str, Any] = {"ReplicationTasks": [MOCK_TASK_RESPONSE_DATA]}
MOCK_DESCRIBE_RESPONSE_WITH_MARKER: dict[str, Any] = {
    "ReplicationTasks": [MOCK_TASK_RESPONSE_DATA],
    "Marker": "marker",
}
MOCK_CREATE_RESPONSE: dict[str, Any] = {"ReplicationTask": MOCK_TASK_RESPONSE_DATA}
MOCK_START_RESPONSE: dict[str, Any] = {"ReplicationTask": {**MOCK_TASK_RESPONSE_DATA, "Status": "starting"}}
MOCK_STOP_RESPONSE: dict[str, Any] = {"ReplicationTask": {**MOCK_TASK_RESPONSE_DATA, "Status": "stopping"}}
MOCK_DELETE_RESPONSE: dict[str, Any] = {"ReplicationTask": {**MOCK_TASK_RESPONSE_DATA, "Status": "deleting"}}


@pytest.fixture
def mock_conn():
    with mock.patch.object(DmsHook, "get_conn") as _get_conn:
        yield _get_conn.return_value


class TestDmsHook:
    def test_init(self):
        hook = DmsHook()

        assert hook.aws_conn_id == "aws_default"

    def test_describe_replication_tasks_with_no_tasks_found(self, mock_conn):
        mock_conn.describe_replication_tasks.return_value = {}

        marker, tasks = DmsHook().describe_replication_tasks()

        mock_conn.describe_replication_tasks.assert_called_once()
        assert marker is None
        assert len(tasks) == 0

    def test_describe_replication_tasks(self, mock_conn):
        mock_conn.describe_replication_tasks.return_value = MOCK_DESCRIBE_RESPONSE
        describe_tasks_kwargs = {
            "Filters": [{"Name": "replication-task-id", "Values": [MOCK_DATA["replication_task_id"]]}]
        }

        marker, tasks = DmsHook().describe_replication_tasks(**describe_tasks_kwargs)

        mock_conn.describe_replication_tasks.assert_called_with(**describe_tasks_kwargs)
        assert marker is None
        assert len(tasks) == 1
        assert tasks[0]["ReplicationTaskArn"] == MOCK_TASK_ARN

    def test_describe_replication_tasks_with_marker(self, mock_conn):
        mock_conn.describe_replication_tasks.return_value = MOCK_DESCRIBE_RESPONSE_WITH_MARKER
        describe_tasks_kwargs = {
            "Filters": [{"Name": "replication-task-id", "Values": [MOCK_DATA["replication_task_id"]]}]
        }

        marker, tasks = DmsHook().describe_replication_tasks(**describe_tasks_kwargs)

        mock_conn.describe_replication_tasks.assert_called_with(**describe_tasks_kwargs)
        assert marker == MOCK_DESCRIBE_RESPONSE_WITH_MARKER["Marker"]
        assert len(tasks) == 1
        assert tasks[0]["ReplicationTaskArn"] == MOCK_TASK_ARN

    def test_find_replication_tasks_by_arn(self, mock_conn):
        mock_conn.describe_replication_tasks.return_value = MOCK_DESCRIBE_RESPONSE
        expected_call_params = {
            "Filters": [{"Name": "replication-task-arn", "Values": [MOCK_TASK_ARN]}],
            "WithoutSettings": False,
        }

        tasks = DmsHook().find_replication_tasks_by_arn(MOCK_TASK_ARN)

        mock_conn.describe_replication_tasks.assert_called_with(**expected_call_params)
        assert len(tasks) == 1
        assert tasks[0]["ReplicationTaskArn"] == MOCK_TASK_ARN

    def test_get_task_status(self, mock_conn):
        mock_conn.describe_replication_tasks.return_value = MOCK_DESCRIBE_RESPONSE
        expected_call_params = {
            "Filters": [{"Name": "replication-task-arn", "Values": [MOCK_TASK_ARN]}],
            "WithoutSettings": True,
        }

        status = DmsHook().get_task_status(MOCK_TASK_ARN)

        mock_conn.describe_replication_tasks.assert_called_with(**expected_call_params)
        assert status == MOCK_TASK_RESPONSE_DATA["Status"]

    def test_get_task_status_with_no_task_found(self, mock_conn):
        mock_conn.describe_replication_tasks.return_value = {}

        status = DmsHook().get_task_status(MOCK_TASK_ARN)

        mock_conn.describe_replication_tasks.assert_called_once()
        assert status is None

    def test_create_replication_task(self, mock_conn):
        mock_conn.create_replication_task.return_value = MOCK_CREATE_RESPONSE
        expected_call_params = {
            "ReplicationTaskIdentifier": MOCK_DATA["replication_task_id"],
            "SourceEndpointArn": MOCK_DATA["source_endpoint_arn"],
            "TargetEndpointArn": MOCK_DATA["target_endpoint_arn"],
            "ReplicationInstanceArn": MOCK_DATA["replication_instance_arn"],
            "MigrationType": MOCK_DATA["migration_type"],
            "TableMappings": json.dumps(MOCK_DATA["table_mappings"]),
        }

        result = DmsHook().create_replication_task(**MOCK_DATA)

        mock_conn.create_replication_task.assert_called_with(**expected_call_params)
        assert result == MOCK_CREATE_RESPONSE["ReplicationTask"]["ReplicationTaskArn"]

    def test_start_replication_task(self, mock_conn):
        mock_conn.start_replication_task.return_value = MOCK_START_RESPONSE
        start_type = "start-replication"
        expected_call_params = {
            "ReplicationTaskArn": MOCK_TASK_ARN,
            "StartReplicationTaskType": start_type,
        }

        DmsHook().start_replication_task(
            replication_task_arn=MOCK_TASK_ARN,
            start_replication_task_type=start_type,
        )

        mock_conn.start_replication_task.assert_called_with(**expected_call_params)

    def test_stop_replication_task(self, mock_conn):
        mock_conn.return_value.stop_replication_task.return_value = MOCK_STOP_RESPONSE
        expected_call_params = {"ReplicationTaskArn": MOCK_TASK_ARN}

        DmsHook().stop_replication_task(replication_task_arn=MOCK_TASK_ARN)

        mock_conn.stop_replication_task.assert_called_with(**expected_call_params)

    def test_delete_replication_task(self, mock_conn):
        mock_conn.delete_replication_task.return_value = MOCK_DELETE_RESPONSE
        expected_call_params = {"ReplicationTaskArn": MOCK_TASK_ARN}

        DmsHook().delete_replication_task(replication_task_arn=MOCK_TASK_ARN)

        mock_conn.delete_replication_task.assert_called_with(**expected_call_params)

    def test_wait_for_task_status_with_unknown_target_status(self):
        with pytest.raises(TypeError, match="Status must be an instance of DmsTaskWaiterStatus"):
            DmsHook().wait_for_task_status(MOCK_TASK_ARN, "unknown_status")

    def test_wait_for_task_status(self, mock_conn):
        expected_waiter_call_params = {
            "Filters": [{"Name": "replication-task-arn", "Values": [MOCK_TASK_ARN]}],
            "WithoutSettings": True,
        }

        DmsHook().wait_for_task_status(replication_task_arn=MOCK_TASK_ARN, status=DmsTaskWaiterStatus.DELETED)

        mock_conn.get_waiter.assert_called_with("replication_task_deleted")
        mock_conn.get_waiter.return_value.wait.assert_called_with(**expected_waiter_call_params)
