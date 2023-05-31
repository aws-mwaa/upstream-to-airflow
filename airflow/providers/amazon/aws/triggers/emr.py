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
import asyncio

from typing import Any

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from botocore.exceptions import WaiterError


class EmrCreateJobFlowTrigger(BaseTrigger):
    """Trigger for EMR Create JobFlow operator."""

    def __init__(
        self,
        job_flow_id: str,
        aws_conn_id: str,
        max_attempts: int | None,
        poll_interval: int | None,
    ):
        self.job_flow_id = job_flow_id
        self.aws_conn_id = aws_conn_id
        self.max_attempts = max_attempts
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.emr.EmrAddStepsTrigger",
            {
                "job_flow_id": str(self.job_flow_id),
                "poll_interval": str(self.poll_interval),
                "max_attempts": str(self.max_attempts),
                "aws_conn_id": str(self.aws_conn_id),
            },
        )

    @cached_property
    def hook(self) -> EmrHook:
        return EmrHook(aws_conn_id=self.aws_conn_id)

    # async def run(self):
    #     attempts = 0
    #     async with self.hook.async_conn as client:
    #         for step_id in self.step_ids:
    #             self.log.info(f"Calling waiter.wait attempt number {attempts}")
    #             await client.get_waiter("step_complete").wait(
    #                 ClusterId=self.job_flow_id,
    #                 StepId=step_id,
    #                 WaiterConfig={
    #                     "Delay": int(self.poll_interval),
    #                     "MaxAttempts": int(self.max_attempts),
    #                 },
    #             )
    #             self.log.info(f"Waiter.wait is successfull with attempt number {attempts}")
    #         yield TriggerEvent({"status": "success", "message": "Steps completed", "step_ids": self.step_ids})
    #         self.log.info(f"This is after yielding{attempts}")



    async def run(self):
        pass