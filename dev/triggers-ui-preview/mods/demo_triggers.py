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
"""Demo triggers for the Triggers page: queue watchers, a job-status wait, and a deadline callback."""

from __future__ import annotations

import asyncio
import datetime
import logging
import os
import random

from botocore.exceptions import ClientError, EndpointConnectionError

from airflow.sdk import BaseOperator
from airflow.triggers.base import BaseEventTrigger, BaseTrigger, TriggerEvent

FLAGS = "/tmp/demo"
log = logging.getLogger("demo_callbacks")


def _flag(name: str) -> bool:
    return os.path.exists(f"{FLAGS}/{name}")


def _take(name: str) -> bool:
    try:
        os.remove(f"{FLAGS}/{name}")
        return True
    except FileNotFoundError:
        return False


class QueueWatcher(BaseEventTrigger):
    """
    Watches a message queue and emits an asset event when messages arrive.

    Demo behaviour, driven by flag files in /tmp/demo: ``<queue>.fire`` delivers a batch,
    ``<queue>.expired`` makes it fail with expired credentials, ``<queue>.down`` with an unreachable endpoint.
    """

    def __init__(self, queue_name: str, region: str = "us-east-1", poll_interval: float = 3.0):
        super().__init__()
        # Not ``queue``: BaseTrigger.queue is the triggerer queue the trigger is assigned to.
        self.queue_name, self.region, self.poll_interval = queue_name, region, poll_interval

    def serialize(self):
        return (
            "demo_triggers.QueueWatcher",
            {"queue_name": self.queue_name, "region": self.region, "poll_interval": self.poll_interval},
        )

    @property
    def url(self) -> str:
        return f"https://sqs.{self.region}.amazonaws.com/123456789012/{self.queue_name}"

    async def run(self):
        self.log.info("Connecting to %s", self.url)
        if _flag(f"{self.queue_name}.expired"):
            self.log.info(
                "Using credentials from role arn:aws:iam::123456789012:role/airflow-%s", self.queue_name
            )
            self.log.warning("Session credentials for %s expired at 2026-11-02T06:00:00Z", self.queue_name)
            raise ClientError(
                {
                    "Error": {
                        "Code": "ExpiredTokenException",
                        "Message": "The security token included in the request is expired",
                    }
                },
                "ReceiveMessage",
            )
        if _flag(f"{self.queue_name}.down"):
            self.log.warning("Retrying ReceiveMessage on %s after a connection reset", self.queue_name)
            raise EndpointConnectionError(endpoint_url=self.url)
        self.log.info("Watching %s every %ss (max 10 messages per poll)", self.queue_name, self.poll_interval)
        polls = 0
        while True:
            polls += 1
            if _take(f"{self.queue_name}.fire"):
                n = random.randint(2, 12)
                self.log.info("Received %d messages from %s; emitting an asset event", n, self.queue_name)
                yield TriggerEvent({"queue": self.queue_name, "messages": n})
                self.log.info("Deleted %d messages from %s", n, self.queue_name)
            elif polls % 10 == 0:
                self.log.info("Polled %s %d times, 0 messages in the last 30s", self.queue_name, polls)
            else:
                self.log.debug("ReceiveMessage on %s returned 0 messages", self.queue_name)
            await asyncio.sleep(self.poll_interval)


class JobStatusTrigger(BaseTrigger):
    """Polls an external job until it finishes, then resumes the deferred task."""

    def __init__(self, job_id: str, poll_interval: float = 5.0, timeout: float = 3600.0):
        super().__init__()
        self.job_id, self.poll_interval, self.timeout = job_id, poll_interval, timeout

    def serialize(self):
        return (
            "demo_triggers.JobStatusTrigger",
            {"job_id": self.job_id, "poll_interval": self.poll_interval, "timeout": self.timeout},
        )

    async def run(self):
        loop = asyncio.get_running_loop()
        end = loop.time() + self.timeout
        polls = 0
        self.log.info("Waiting for job %s to finish", self.job_id)
        while loop.time() < end:
            polls += 1
            pct = min(99, polls)
            self.log.info("Job %s is RUNNING (%d%% done, poll %d)", self.job_id, pct, polls)
            await asyncio.sleep(self.poll_interval)
        self.log.info("Job %s SUCCEEDED", self.job_id)
        yield TriggerEvent({"job_id": self.job_id, "status": "SUCCEEDED"})


class WaitForJobOperator(BaseOperator):
    """Starts an external job, then defers to :class:`JobStatusTrigger` until it finishes."""

    def __init__(self, *, job: str, timeout: float = 3600.0, **kwargs):
        super().__init__(**kwargs)
        self.job, self.timeout = job, timeout

    def execute(self, context):
        job_id = f"{self.job}-{datetime.date.today():%Y%m%d}"
        self.log.info("Started job %s; deferring until it finishes", job_id)
        self.defer(trigger=JobStatusTrigger(job_id=job_id, timeout=self.timeout), method_name="done")

    def done(self, context, event=None):
        self.log.info("Job finished: %s", event)
        return event


async def page_on_call(dag_id: str = "", seconds: float = 3600, **kwargs):
    """A deadline's AsyncCallback, run by the triggerer: pages on-call, then waits for an acknowledgement."""
    log.info("Deadline missed for %s; paging the data-platform on-call", dag_id)
    for n in range(1, int(seconds // 10) + 1):
        await asyncio.sleep(10)
        log.info("Waiting for the on-call to acknowledge the page (%ds)", n * 10)
    return "acknowledged"
