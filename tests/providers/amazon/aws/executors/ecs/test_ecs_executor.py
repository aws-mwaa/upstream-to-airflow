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

import datetime as dt
import os
from unittest import mock

import pytest

from airflow.providers.amazon.aws.executors.ecs import (
    AwsEcsExecutor,
    BotoTaskSchema,
    EcsExecutorTask,
    EcsTaskCollection,
)
from airflow.utils.state import State

from .botocore_helper import assert_botocore_call, get_botocore_model


def set_conf():
    os.environ["AIRFLOW__ECS_EXECUTOR__REGION"] = "us-west-1"
    os.environ["AIRFLOW__ECS_EXECUTOR__CLUSTER"] = "some-cluster"
    os.environ["AIRFLOW__ECS_EXECUTOR__CONTAINER_NAME"] = "some-container-name"
    os.environ["AIRFLOW__ECS_EXECUTOR__TASK_DEFINITION"] = "some-task-def"
    os.environ["AIRFLOW__ECS_EXECUTOR__LAUNCH_TYPE"] = "FARGATE"
    os.environ["AIRFLOW__ECS_EXECUTOR__PLATFORM_VERSION"] = "LATEST"
    os.environ["AIRFLOW__ECS_EXECUTOR__ASSIGN_PUBLIC_IP"] = "DISABLED"
    os.environ["AIRFLOW__ECS_EXECUTOR__SECURITY_GROUPS"] = "sg1,sg2"
    os.environ["AIRFLOW__ECS_EXECUTOR__SUBNETS"] = "sub1,sub2"


def unset_conf():
    for env in os.environ:
        if env.startswith("AIRFLOW__ECS_EXECUTOR__"):
            os.environ.pop(env)


ARN1 = "arn1"
ARN2 = "arn2"


def mock_task(arn=ARN1):
    return mock.Mock(spec=EcsExecutorTask, task_arn=arn)


# These first two fixtures look unusual.  For tests which do not care if the object
# returned by the fixture is unique, use it like a normal fixture.  If your test
# requires a unique value, then call it like a method.
#
# See `test_info_by_key` for an example of a test that requires two unique mocked queues.


@pytest.fixture(autouse=True)
def mock_airflow_key():
    def _key():
        return mock.Mock(spec=tuple)

    return _key


@pytest.fixture(autouse=True)
def mock_queue():
    def _queue():
        return mock.Mock(spec=str)

    return _queue


# The following two fixtures look different because no exitsting test
# cares if they have unique values, so the same value is always used.
@pytest.fixture(autouse=True)
def mock_airflow_cmd():
    return mock.Mock(spec=list)


@pytest.fixture(autouse=True)
def mock_config():
    return mock.Mock(spec=dict)


class TestEcsTaskCollection:
    """Tests EcsTaskCollection Class."""

    # You can't use a fixture in setup_method unless you declare setup_method to be a fixture itself.
    @pytest.fixture(autouse=True)
    def setup_method(self, mock_airflow_key):
        # Create a new Collection and verify it is empty.
        self.collection = EcsTaskCollection()
        assert len(self.collection) == 0

        # Generate two mock keys and assert they are different.  If the value
        # of the key does not matter for a test, let it use the auto-fixture.
        self.key1 = mock_airflow_key()
        self.key2 = mock_airflow_key()
        assert self.key1 != self.key2

    def test_add_task(self):
        # Add a task, verify that the collection has grown and the task arn matches.
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_airflow_cmd, mock_config)
        assert len(self.collection) == 1
        assert self.collection.tasks[ARN1].task_arn == ARN1

        # Add a task, verify that the collection has grown and the task arn is not the same as the first.
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_airflow_cmd, mock_config)
        assert len(self.collection) == 2
        assert self.collection.tasks[ARN2].task_arn == ARN2
        assert self.collection.tasks[ARN2].task_arn != self.collection.tasks[ARN1].task_arn

    def test_task_by_key(self):
        self.collection.add_task(mock_task(), mock_airflow_key, mock_queue, mock_airflow_cmd, mock_config)

        task = self.collection.task_by_key(mock_airflow_key)

        assert task == self.collection.tasks[ARN1]

    def test_task_by_arn(self):
        self.collection.add_task(mock_task(), mock_airflow_key, mock_queue, mock_airflow_cmd, mock_config)

        task = self.collection.task_by_arn(ARN1)

        assert task == self.collection.tasks[ARN1]

    def test_info_by_key(self, mock_queue):
        self.collection.add_task(
            mock_task(ARN1), self.key1, queue1 := mock_queue(), mock_airflow_cmd, mock_config
        )
        self.collection.add_task(
            mock_task(ARN2), self.key2, queue2 := mock_queue(), mock_airflow_cmd, mock_config
        )
        assert queue1 != queue2

        task1_info = self.collection.info_by_key(self.key1)
        assert task1_info.queue == queue1
        assert task1_info.cmd == mock_airflow_cmd
        assert task1_info.config == mock_config

        task2_info = self.collection.info_by_key(self.key2)
        assert task2_info.queue == queue2
        assert task2_info.cmd == mock_airflow_cmd
        assert task2_info.config == mock_config

        assert task1_info != task2_info

    def test_get_all_arns(self):
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_airflow_cmd, mock_config)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_airflow_cmd, mock_config)

        assert self.collection.get_all_arns() == [ARN1, ARN2]

    def test_get_all_task_keys(self):
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_airflow_cmd, mock_config)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_airflow_cmd, mock_config)

        assert self.collection.get_all_task_keys() == [self.key1, self.key2]

    def test_pop_by_key(self):
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_airflow_cmd, mock_config)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_airflow_cmd, mock_config)
        task1_as_saved = self.collection.tasks[ARN1]

        assert len(self.collection) == 2
        task1_as_popped = self.collection.pop_by_key(self.key1)
        assert len(self.collection) == 1
        # Assert it returns the same task.
        assert task1_as_popped == task1_as_saved
        # Assert the popped task is removed.
        with pytest.raises(KeyError):
            assert self.collection.task_by_key(self.key1)
        # Assert the remaining task is task2.
        assert self.collection.task_by_key(self.key2)

    def test_update_task(self):
        self.collection.add_task(
            initial_task := mock_task(), mock_airflow_key, mock_queue, mock_airflow_cmd, mock_config
        )
        assert self.collection[ARN1] == initial_task
        self.collection.update_task(updated_task := mock_task())

        assert self.collection[ARN1] == updated_task
        assert initial_task != updated_task

    def test_failure_count(self):
        # Create a new Collection and add a two tasks.
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_airflow_cmd, mock_config)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_airflow_cmd, mock_config)

        assert self.collection.failure_count_by_key(self.key1) == 0
        for i in range(5):
            self.collection.increment_failure_count(self.key1)
            assert self.collection.failure_count_by_key(self.key1) == i + 1
        assert self.collection.failure_count_by_key(self.key2) == 0


class TestEcsExecutorTask:
    """Tests the EcsExecutorTask DTO."""

    def test_queued_tasks(self):
        """Tasks that are pending launch identified as 'queued'"""
        queued_tasks = [
            EcsExecutorTask(
                task_arn="AAA", last_status="PROVISIONING", desired_status="RUNNING", containers=[{}]
            ),
            EcsExecutorTask(task_arn="BBB", last_status="PENDING", desired_status="RUNNING", containers=[{}]),
            EcsExecutorTask(
                task_arn="CCC", last_status="ACTIVATING", desired_status="RUNNING", containers=[{}]
            ),
        ]
        for task in queued_tasks:
            assert State.QUEUED == task.get_task_state()

    def test_running_tasks(self):
        """Tasks that have been launched are identified as 'running'"""
        running_task = EcsExecutorTask(
            task_arn="AAA", last_status="RUNNING", desired_status="RUNNING", containers=[{}]
        )
        assert State.RUNNING == running_task.get_task_state()

    def test_removed_tasks(self):
        """Tasks that failed to launch are identified as 'removed'"""
        deprovisioning_tasks = [
            EcsExecutorTask(
                task_arn="DDD", last_status="DEACTIVATING", desired_status="STOPPED", containers=[{}]
            ),
            EcsExecutorTask(
                task_arn="EEE", last_status="STOPPING", desired_status="STOPPED", containers=[{}]
            ),
            EcsExecutorTask(
                task_arn="FFF", last_status="DEPROVISIONING", desired_status="STOPPED", containers=[{}]
            ),
        ]
        for task in deprovisioning_tasks:
            assert State.REMOVED == task.get_task_state()

        removed_task = EcsExecutorTask(
            task_arn="DEAD",
            last_status="STOPPED",
            desired_status="STOPPED",
            containers=[{}],
            stopped_reason="Timeout waiting for network interface provisioning to complete.",
        )
        assert State.REMOVED == removed_task.get_task_state()

    def test_stopped_tasks(self):
        """Tasks that have terminated are identified as either 'success' or 'failure'"""
        successful_container = {"exit_code": 0, "last_status": "STOPPED"}
        error_container = {"exit_code": 100, "last_status": "STOPPED"}

        for status in ("DEACTIVATING", "STOPPING", "DEPROVISIONING", "STOPPED"):
            success_task = EcsExecutorTask(
                task_arn="GOOD",
                last_status=status,
                desired_status="STOPPED",
                stopped_reason="Essential container in task exited",
                started_at=dt.datetime.now(),
                containers=[successful_container],
            )
            assert State.SUCCESS == success_task.get_task_state()

        for status in ("DEACTIVATING", "STOPPING", "DEPROVISIONING", "STOPPED"):
            failed_task = EcsExecutorTask(
                task_arn="FAIL",
                last_status=status,
                desired_status="STOPPED",
                stopped_reason="Essential container in task exited",
                started_at=dt.datetime.now(),
                containers=[successful_container, successful_container, error_container],
            )
            assert State.FAILED == failed_task.get_task_state()


class TestAwsEcsExecutor:
    """Tests the AWS ECS Executor."""

    def setup_method(self) -> None:
        """Creates Botocore Loader (used for asserting botocore calls) and a mocked ecs client"""
        set_conf()
        self.ecs_model = get_botocore_model("ecs")
        self.__set_mocked_executor()

    @classmethod
    def teardown_method(cls) -> None:
        unset_conf()

    def test_execute(self):
        """Test execution from end-to-end"""
        airflow_key = mock.Mock(spec=tuple)
        airflow_cmd = mock.Mock(spec=list)

        self.executor.ecs.run_task.return_value = {
            "tasks": [
                {
                    "taskArn": "001",
                    "lastStatus": "",
                    "desiredStatus": "",
                    "containers": [{"name": "some-ecs-container"}],
                }
            ],
            "failures": [],
        }

        assert 0 == len(self.executor.pending_tasks)
        self.executor.execute_async(airflow_key, airflow_cmd)
        assert 1 == len(self.executor.pending_tasks)

        self.executor.attempt_task_runs()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.ecs.run_task.assert_called_once()
        self.assert_botocore_call("RunTask", *self.executor.ecs.run_task.call_args)

        # task is stored in active worker
        assert 1 == len(self.executor.active_workers)
        assert "001" in self.executor.active_workers.task_by_key(airflow_key).task_arn

    def test_failed_execute_api(self):
        """Test what happens when ECS refuses to execute a task."""
        self.executor.ecs.run_task.return_value = {
            "tasks": [],
            "failures": [
                {"arn": "001", "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }

        airflow_key = mock.Mock(spec=tuple)
        airflow_cmd = mock.Mock(spec=list)
        self.executor.execute_async(airflow_key, airflow_cmd)

        # no matter what, don't schedule until run_task becomes successful
        for _ in range(self.executor.MAX_FAILURE_CHECKS * 2):
            self.executor.attempt_task_runs()
            # task is not stored in active workers
            assert len(self.executor.active_workers) == 0

    @mock.patch("airflow.executors.base_executor.BaseExecutor.fail")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.success")
    def test_sync(self, success_mock, fail_mock):
        """Test synch from end-to-end"""
        response_json = self.__mock_sync()
        formatted_response_json = BotoTaskSchema().load(response_json)
        assert State.SUCCESS == formatted_response_json.get_task_state()

        self.executor.sync_running_tasks()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.ecs.describe_tasks.assert_called_once()
        self.assert_botocore_call("DescribeTasks", *self.executor.ecs.describe_tasks.call_args)

        # task is not stored in active workers
        assert len(self.executor.active_workers) == 0
        # Task is immediately succeeded
        success_mock.assert_called_once()
        fail_mock.assert_not_called()

    @mock.patch("airflow.executors.base_executor.BaseExecutor.fail")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.success")
    def test_failed_sync(self, success_mock, fail_mock):
        """Test success and failure states"""
        response_json = self.__mock_sync()

        # set container's exit code to failure
        response_json["containers"][0]["exitCode"] = 100
        assert State.FAILED == BotoTaskSchema().load(response_json).get_task_state()
        self.executor.sync()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.ecs.describe_tasks.assert_called_once()
        self.assert_botocore_call("DescribeTasks", *self.executor.ecs.describe_tasks.call_args)

        # task is not stored in active workers
        assert len(self.executor.active_workers) == 0
        # Task is immediately succeeded
        fail_mock.assert_called_once()
        success_mock.assert_not_called()

    @mock.patch("airflow.executors.base_executor.BaseExecutor.fail")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.success")
    def test_failed_sync_api(self, success_mock, fail_mock):
        """Test what happens when ECS sync fails for certain tasks repeatedly"""
        self.__mock_sync()
        self.executor.ecs.describe_tasks.return_value = {
            "tasks": [],
            "failures": [
                {"arn": "ABC", "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }

        # Call Sync 3 times with failures
        for check_count in range(AwsEcsExecutor.MAX_FAILURE_CHECKS):
            self.executor.sync_running_tasks()
            # ensure that run_task is called correctly as defined by Botocore docs
            assert self.executor.ecs.describe_tasks.call_count == check_count + 1
            self.assert_botocore_call("DescribeTasks", *self.executor.ecs.describe_tasks.call_args)

            # Ensure task arn is not removed from active
            assert "ABC" in self.executor.active_workers.get_all_arns()

            # Task is not failed or succeeded
            fail_mock.assert_not_called()
            success_mock.assert_not_called()

        # Last call should fail the task
        self.executor.sync_running_tasks()
        assert "ABC" not in self.executor.active_workers.get_all_arns()
        fail_mock.assert_called()
        success_mock.asswer_not_called()

    def test_terminate(self):
        """Test that executor can shut everything down; forcing all tasks to unnaturally exit."""
        response_task = self.__mock_sync()
        response_task["containers"][0]["exitCode"] = 100
        assert State.FAILED == BotoTaskSchema().load(response_task).get_task_state()

        self.executor.terminate()

        self.executor.ecs.stop_task.assert_called()
        self.assert_botocore_call("StopTask", *self.executor.ecs.stop_task.call_args)

    def assert_botocore_call(self, method_name, args, kwargs):
        assert_botocore_call(self.ecs_model, method_name, args, kwargs)

    def test_end(self):
        """Test that executor can end successfully; waiting for all tasks to naturally exit."""
        sync_call_count = 0
        sync_func = self.executor.sync

        def sync_mock():
            """Mock won't work here, because we actually want to call the 'sync' func."""
            nonlocal sync_call_count
            sync_func()
            sync_call_count += 1

        self.executor.sync = sync_mock
        response_task = self.__mock_sync()
        response_task["containers"][0]["exitCode"] = 100
        self.executor.end(heartbeat_interval=0)

        self.executor.sync = sync_func

    def __set_mocked_executor(self):
        """Mock ECS such that there's nothing wrong with anything."""
        executor = AwsEcsExecutor()
        executor.start()

        # replace boto3 ecs client with mock
        ecs_mock = mock.Mock(spec=executor.ecs)
        run_task_ret_val = {"tasks": [{"taskArn": "001"}], "failures": []}
        ecs_mock.run_task.return_value = run_task_ret_val
        executor.ecs = ecs_mock

        self.executor = executor

    def __mock_sync(self):
        """Mock ECS such that there's nothing wrong with anything."""

        initial_task = mock.Mock(spec=EcsExecutorTask)
        initial_task.task_arn = "ABC"
        initial_task.api_failure_count = 0
        initial_task.get_task_state.return_value = State.RUNNING

        airflow_cmd = mock.Mock(spec=list)
        airflow_key = mock.Mock(spec=tuple)
        airflow_queue = mock.Mock(spec=str)
        airflow_exec_conf = mock.Mock(spec=dict)
        self.executor.active_workers.add_task(
            initial_task, airflow_key, airflow_queue, airflow_cmd, airflow_exec_conf
        )

        response_task_json = {
            "taskArn": "ABC",
            "desiredStatus": "STOPPED",
            "lastStatus": "STOPPED",
            "startedAt": dt.datetime.now(),
            "containers": [{"name": "some-ecs-container", "lastStatus": "STOPPED", "exitCode": 0}],
        }
        self.executor.ecs.describe_tasks.return_value = {"tasks": [response_task_json], "failures": []}
        return response_task_json
