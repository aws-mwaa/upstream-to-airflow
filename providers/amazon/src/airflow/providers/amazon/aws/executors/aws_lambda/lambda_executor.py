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

import datetime
import json
import time
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from boto3.session import NoCredentialsError
from botocore.utils import ClientError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.aws_lambda.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    INVALID_CREDENTIALS_EXCEPTIONS,
    AllLambdaConfigKeys,
    CommandType,
    ExecutorConfigType,
)
from airflow.providers.amazon.aws.executors.utils.exponential_backoff_retry import exponential_backoff_retry
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


@dataclass
class LambdaQueuedTask:
    """Represents an Lambda task that is queued. The task will be run in the next heartbeat."""

    key: TaskInstanceKey
    command: CommandType
    queue: str
    executor_config: ExecutorConfigType
    attempt_number: int
    next_attempt_time: datetime.datetime


class LambdaExecutor(BaseExecutor):
    """
    An Airflow Executor that submits tasks to AWS Lambda asynchronously.

    When execute_async() is called, the executor invokes a specified AWS Lambda function (asynchronously)
    with a payload that includes the task command and a unique task key.

    This implementation uses only SQS: the Lambda function writes its result directly to an SQS queue,
    which is then polled by this executor to update task state in Airflow.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pending_tasks: deque = deque()
        self.running_tasks: dict = {}
        self.lambda_function_name = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.FUNCTION_NAME)
        self.sqs_queue_url = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.SQS_QUEUE_URL, fallback="")
        self.qualifier = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.QUALIFIER, fallback="")

        self.attempts_since_last_successful_connection = 0
        self.IS_BOTO_CONNECTION_HEALTHY = False
        self.load_connections(check_connection=False)

    def start(self):
        """Call this when the Executor is run for the first time by the scheduler."""
        check_health = conf.getboolean(
            CONFIG_GROUP_NAME, AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP, fallback=False
        )

        if not check_health:
            return

        self.log.info("Starting Lambda Executor and determining health...")
        try:
            self.check_health()
        except AirflowException:
            self.log.error("Stopping the Airflow Scheduler from starting until the issue is resolved.")
            raise

    def check_health(self):
        """
        Check the health of the Lambda and SQS connections.

        For lambda: Use list_functions to test if the lambda connection works and might as well check if the function
        name we were given is present in the response.
        For SQS: Use list_queues to test if the SQS connection works and might as well check if the queue
        name we were given is present in the response.
        """
        self.IS_BOTO_CONNECTION_HEALTHY = False

        try:
            self.log.info("Checking Lambda and SQS connections")
            lambda_list_response = self.lambda_client.list_functions()
            if self.lambda_function_name not in [
                function["FunctionName"] for function in lambda_list_response["Functions"]
            ]:
                raise AirflowException("Lambda function %s not found.", self.lambda_function_name)
            self.log.info(
                "Lambda connection is healthy and function %s is present.", self.lambda_function_name
            )
            sqs_list_response = self.sqs_client.list_queues()
            if self.sqs_queue_url not in [queue_url for queue_url in sqs_list_response.get("QueueUrls", [])]:
                raise AirflowException("SQS queue %s not found.", self.sqs_queue_url)
            self.log.info("SQS connection is healthy and queue %s is present.", self.sqs_queue_url)

            # If we reach this point, both connections are healthy and all resources are present
            self.IS_BOTO_CONNECTION_HEALTHY = True
        except Exception:
            self.log.exception("Lambda Executor health check failed")
            raise AirflowException(
                "The Lambda executor will not be able to run Airflow tasks until the issue is addressed."
            )

    def load_connections(self, check_connection: bool = True):
        """
        Retrieve the AWS connection via Hooks to leverage the Airflow connection system.

        param check_connection: If True, check the health of the connection after loading it.
        """
        self.log.info("Loading Connections")
        aws_conn_id = conf.get(
            CONFIG_GROUP_NAME,
            AllLambdaConfigKeys.AWS_CONN_ID,
            fallback=CONFIG_DEFAULTS[AllLambdaConfigKeys.AWS_CONN_ID],
        )
        region_name = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.REGION_NAME, fallback=None)
        self.sqs_client = SqsHook(aws_conn_id=aws_conn_id, region_name=region_name).conn
        self.lambda_client = LambdaHook(aws_conn_id=aws_conn_id, region_name=region_name).conn

        self.attempts_since_last_successful_connection += 1
        self.last_connection_reload = timezone.utcnow()

        if check_connection:
            self.check_health()
            self.attempts_since_last_successful_connection = 0

    def sync(self):
        """
        Sync the executor with the current state of tasks.

        Check in on currently running tasks and attempt to run any new tasks that have been queued.
        """
        if not self.IS_BOTO_CONNECTION_HEALTHY:
            exponential_backoff_retry(
                self.last_connection_reload,
                self.attempts_since_last_successful_connection,
                self.load_connections,
            )
            if not self.IS_BOTO_CONNECTION_HEALTHY:
                return
        try:
            self.sync_running_tasks()
            self.attempt_task_runs()
        except (ClientError, NoCredentialsError) as error:
            error_code = error.response["Error"]["Code"]
            if error_code in INVALID_CREDENTIALS_EXCEPTIONS:
                self.IS_BOTO_CONNECTION_HEALTHY = False
                self.log.warning(
                    "AWS credentials are either missing or expired: %s.\nRetrying connection", error
                )
        except Exception as error:
            self.log.exception("An error occurred while syncing tasks: %s", error)

    def execute_async(self, key: TaskInstanceKey, command: CommandType, queue=None, executor_config=None):
        """
        Save the task to be executed in the next sync by inserting the commands into a queue.

        :param key: A unique task key (typically a tuple identifying the task instance).
        :param command: The shell command string to execute.
        :param executor_config: Passed along to the lambda function json serialized as the Context object.
        :param queue: (Unused) to keep the same signature as the base.
        """
        # TODO: executor config used for context, does that make sense?

        self.pending_tasks.append(
            LambdaQueuedTask(
                key, command, queue if queue else "", executor_config or {}, 1, timezone.utcnow()
            )
        )

    def attempt_task_runs(self):
        """
        Attempt to run tasks that are queued in the pending_tasks deque.

        Each task is submitted to AWS Lambda with a payload containing the task key and command.
        The task key is used to track the task's state in Airflow.
        """
        queue_len = len(self.pending_tasks)
        # failure_reasons = defaultdict(int)
        for _ in range(queue_len):
            task_to_run = self.pending_tasks.popleft()
            task_key = task_to_run.key
            cmd = task_to_run.command
            exec_config = task_to_run.executor_config
            # TODO: Add logic to handle task attempts and backoff
            # attempt_number = task_to_run.attempt_number
            # failure_reasons = []
            ser_task_key = json.dumps(task_key._asdict())
            payload = {
                "task_key": ser_task_key,
                "command": cmd,
            }
            self.log.info("Submitting task %s to Lambda function %s", task_key, self.lambda_function_name)
            invoke_kwargs = {
                "FunctionName": self.lambda_function_name,
                "InvocationType": "Event",
                "Payload": json.dumps(payload),
                "ClientContext": json.dumps(exec_config),
            }
            if self.qualifier:
                invoke_kwargs["Qualifier"] = self.qualifier
            response = self.lambda_client.invoke(**invoke_kwargs)

            status_code = response.get("StatusCode")
            self.log.info("Invoked Lambda for task %s with status %s", task_key, status_code)
            self.running_tasks[ser_task_key] = task_key
            try:
                # Add the serialized task key as the info, this will be assigned on the ti as the external_executor_id
                self.running_state(task_key, ser_task_key)
            except AttributeError:
                # running_state is newly added, and only needed to support task adoption (an optional
                # executor feature).
                # TODO: remove when min airflow version >= 2.9.2
                pass

    def sync_running_tasks(self):
        """
        Poll the SQS queue for messages indicating task completion.

        Each message is expected to contain a JSON payload with 'task_key' and 'return_code'.
        Based on the return code, update the task state accordingly.
        """
        if not len(self.running_tasks):
            # self.log.debug("No tasks to process.")
            # TODO: Don't log this in prod, remove this before PR
            self.log.info("No tasks to process.")
            return
        response = self.sqs_client.receive_message(
            QueueUrl=self.sqs_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,  # Short poll; adjust if needed.
        )
        messages = response.get("Messages", [])
        for message in messages:
            receipt_handle = message["ReceiptHandle"]
            body = json.loads(message["Body"])
            task_key = body.get("task_key")
            return_code = body.get("return_code")

            if return_code == 0:
                self.change_state(self.running_tasks[task_key], State.SUCCESS)
                self.log.info("Task %s received from SQS queue has succeeded.", task_key)
            else:
                self.change_state(task_key, State.FAILED)
                self.log.info(
                    "Task %s received from SQS queue has failed with return code %s",
                    task_key,
                    return_code,
                )
            # Remove the task from the tracking mapping.
            self.running_tasks.pop(str(task_key))
            # Delete the message from the queue.
            self.sqs_client.delete_message(QueueUrl=self.sqs_queue_url, ReceiptHandle=receipt_handle)

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        """
        Adopt task instances which have an external_executor_id (the serialized task key).

        Anything that is not adopted will be cleared by the scheduler and becomes eligible for re-scheduling.

        :param tis: The task instances to adopt.
        """
        with Stats.timer("lambda_executor.adopt_task_instances.duration"):
            adopted_tis: list[TaskInstance] = []

            if serialized_task_keys := [
                (ti, ti.external_executor_id) for ti in tis if ti.external_executor_id
            ]:
                for ti, ser_task_key in serialized_task_keys:
                    task_key = TaskInstanceKey.from_dict(json.loads(ser_task_key))
                    self.running_tasks[ser_task_key] = task_key
                    adopted_tis.append(ti)

            if adopted_tis:
                tasks = [f"{task} in state {task.state}" for task in adopted_tis]
                task_instance_str = "\n\t".join(tasks)
                self.log.info(
                    "Adopted the following %d tasks from a dead executor:\n\t%s",
                    len(adopted_tis),
                    task_instance_str,
                )

            not_adopted_tis = [ti for ti in tis if ti not in adopted_tis]
            return not_adopted_tis

    def end(self, heartbeat_interval=10):
        """
        End execution. Poll until all outstanding tasks are marked as completed.

        This is a blocking call that will wait until all tasks are completed or the executor is stopped.

        param heartbeat_interval: The interval in seconds to wait between checks for task completion.
        """
        self.log.info("Finish waiting for outstanding tasks.")
        while True:
            self.sync()
            if not self.running_tasks:
                break
            self.log.info("Waiting for %d tasks to complete.", len(self.running_tasks))
            time.sleep(heartbeat_interval)
            # TODO: should we add a timeout here?
        self.log.info("All tasks completed; executor ending.")
