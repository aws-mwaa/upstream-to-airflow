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

from typing import Any

from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class RedshiftCreateClusterTrigger(BaseTrigger):
    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int,
        max_attempt: int,
        aws_conn_id: str,
    ):
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.max_attempt = max_attempt
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftCreateClusterTrigger",
            {
                "cluster_identifier": str(self.cluster_identifier),
                "poll_interval": str(self.poll_interval),
                "max_attempt": str(self.max_attempt),
                "aws_conn_id": str(self.aws_conn_id),
            },
        )

    async def run(self):
        self.redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        async with self.redshift_hook.async_conn as client:
            await client.get_waiter("cluster_available").wait(
                ClusterIdentifier=self.cluster_identifier,
                WaiterConfig={
                    "Delay": int(self.poll_interval),
                    "MaxAttempts": int(self.max_attempt),
                },
            )
        yield TriggerEvent({"status": "success", "message": "Cluster Created"})


class RedshiftCreateClusterSnapshotTrigger(BaseTrigger):
    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int,
        max_attempt: int,
        aws_conn_id: str,
    ):
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.max_attempt = max_attempt
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftCreateClusterSnapshotTrigger",
            {
                "cluster_identifier": str(self.cluster_identifier),
                "poll_interval": str(self.poll_interval),
                "max_attempt": str(self.max_attempt),
                "aws_conn_id": str(self.aws_conn_id),
            },
        )

    async def run(self):
        self.redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        async with self.redshift_hook.async_conn as client:
            await client.get_waiter("snapshot_available").wait(
                ClusterIdentifier=self.cluster_identifier,
                WaiterConfig={
                    "Delay": int(self.poll_interval),
                    "MaxAttempts": int(self.max_attempt),
                },
            )
        yield TriggerEvent({"status": "success", "message": "Snapshot Created"})


class RedshiftDeleteClusterTrigger(BaseTrigger):
    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int,
        max_attempt: int,
        aws_conn_id: str,
    ):
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.max_attempt = max_attempt
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftDeleteClusterTrigger",
            {
                "cluster_identifier": str(self.cluster_identifier),
                "poll_interval": str(self.poll_interval),
                "max_attempt": str(self.max_attempt),
                "aws_conn_id": str(self.aws_conn_id),
            },
        )

    async def run(self):
        self.redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        async with self.redshift_hook.async_conn as client:
            await client.get_waiter("cluster_deleted").wait(
                ClusterIdentifier=self.cluster_identifier,
                WaiterConfig={
                    "Delay": int(self.poll_interval),
                    "MaxAttempts": int(self.max_attempt),
                },
            )
        yield TriggerEvent({"status": "success", "message": "Cluster deleted"})


class RedshiftResumeClusterTrigger(BaseTrigger):
    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int,
        max_attempt: int,
        aws_conn_id: str,
    ):
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.max_attempt = max_attempt
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftResumeClusterTrigger",
            {
                "cluster_identifier": str(self.cluster_identifier),
                "poll_interval": str(self.poll_interval),
                "max_attempt": str(self.max_attempt),
                "aws_conn_id": str(self.aws_conn_id),
            },
        )

    async def run(self):
        self.redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        async with self.redshift_hook.async_conn as client:
            waiter = self.redshift_hook.get_waiter("cluster_resumed", deferrable=True, client=client)
            await waiter.wait(
                ClusterIdentifier=self.cluster_identifier,
                WaiterConfig={
                    "Delay": int(self.poll_interval),
                    "MaxAttempts": int(self.max_attempt),
                },
            )
        yield TriggerEvent({"status": "success", "message": "Cluster resumed"})


class RedshiftPauseClusterTrigger(BaseTrigger):
    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int,
        max_attempt: int,
        aws_conn_id: str,
    ):
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.max_attempt = max_attempt
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftPauseClusterTrigger",
            {
                "cluster_identifier": str(self.cluster_identifier),
                "poll_interval": str(self.poll_interval),
                "max_attempt": str(self.max_attempt),
                "aws_conn_id": str(self.aws_conn_id),
            },
        )

    async def run(self):
        self.redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        async with self.redshift_hook.async_conn as client:
            waiter = self.redshift_hook.get_waiter("cluster_paused", deferrable=True, client=client)
            await waiter.wait(
                ClusterIdentifier=self.cluster_identifier,
                WaiterConfig={
                    "Delay": int(self.poll_interval),
                    "MaxAttempts": int(self.max_attempt),
                },
            )
        yield TriggerEvent({"status": "success", "message": "Cluster paused"})
