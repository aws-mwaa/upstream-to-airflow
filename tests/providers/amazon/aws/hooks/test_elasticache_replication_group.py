#
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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.elasticache_replication_group import ElastiCacheReplicationGroupHook

REPLICATION_GROUP_ID = "test-elasticache-replication-group-hook"

REPLICATION_GROUP_CONFIG = {
    "ReplicationGroupId": REPLICATION_GROUP_ID,
    "ReplicationGroupDescription": REPLICATION_GROUP_ID,
    "AutomaticFailoverEnabled": False,
    "NumCacheClusters": 1,
    "CacheNodeType": "cache.m5.large",
    "Engine": "redis",
    "EngineVersion": "5.0.4",
    "CacheParameterGroupName": "default.redis5.0",
}

VALID_STATES = frozenset(
    {"creating", "available", "modifying", "deleting", "create - failed", "snapshotting"}
)


@pytest.fixture
def mock_conn():
    with mock.patch.object(ElastiCacheReplicationGroupHook, "get_conn") as _get_conn:
        _get_conn.return_value.create_replication_group.return_value = {
            "ReplicationGroup": {"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "creating"}
        }
        yield _get_conn.return_value


class TestElastiCacheReplicationGroupHook:
    def test_conn_not_none(self):
        hook = ElastiCacheReplicationGroupHook()
        assert hook.conn is not None

    def test_create_replication_group(self, mock_conn):
        response = ElastiCacheReplicationGroupHook().create_replication_group(config=REPLICATION_GROUP_CONFIG)

        assert response["ReplicationGroup"]["ReplicationGroupId"] == REPLICATION_GROUP_ID
        assert response["ReplicationGroup"]["Status"] == "creating"

    def test_describe_replication_group(self, mock_conn):
        mock_conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [{"ReplicationGroupId": REPLICATION_GROUP_ID}]
        }

        response = ElastiCacheReplicationGroupHook().describe_replication_group(
            replication_group_id=REPLICATION_GROUP_ID
        )
        assert response["ReplicationGroups"][0]["ReplicationGroupId"] == REPLICATION_GROUP_ID

    def test_get_replication_group_status(self, mock_conn):
        mock_conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [{"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "available"}]
        }

        response = ElastiCacheReplicationGroupHook().get_replication_group_status(
            replication_group_id=REPLICATION_GROUP_ID
        )
        assert response in VALID_STATES

    def test_is_replication_group_available(self, mock_conn):
        mock_conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [{"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "available"}]
        }

        response = ElastiCacheReplicationGroupHook().is_replication_group_available(
            replication_group_id=REPLICATION_GROUP_ID
        )
        assert response in (True, False)

    def test_wait_for_availability(self, mock_conn):
        # Test non availability
        mock_conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [{"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "creating"}]
        }

        response = ElastiCacheReplicationGroupHook().wait_for_availability(
            replication_group_id=REPLICATION_GROUP_ID,
            max_retries=1,
            initial_sleep_time=1,  # seconds
        )
        assert response is False

        # Test availability
        mock_conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [{"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "available"}]
        }

        response = ElastiCacheReplicationGroupHook().wait_for_availability(
            replication_group_id=REPLICATION_GROUP_ID,
            max_retries=1,
            initial_sleep_time=1,  # seconds
        )
        assert response is True

    def test_delete_replication_group(self, mock_conn):
        mock_conn.delete_replication_group.return_value = {
            "ReplicationGroup": {"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "deleting"}
        }
        # Wait for availability, can only delete when replication group is available
        mock_conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [{"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "available"}]
        }

        response = ElastiCacheReplicationGroupHook().wait_for_availability(
            replication_group_id=REPLICATION_GROUP_ID,
            max_retries=1,
            initial_sleep_time=1,  # seconds
        )
        assert response is True

        response = ElastiCacheReplicationGroupHook().delete_replication_group(
            replication_group_id=REPLICATION_GROUP_ID
        )
        assert response["ReplicationGroup"]["ReplicationGroupId"] == REPLICATION_GROUP_ID
        assert response["ReplicationGroup"]["Status"] == "deleting"

    @staticmethod
    def _raise_replication_group_not_found_exp(mock_conn):
        mock_conn.exceptions.ReplicationGroupNotFoundFault = BaseException
        return mock_conn.exceptions.ReplicationGroupNotFoundFault

    # noinspection PyUnusedLocal
    def _mock_describe_side_effect(self, mock_conn):
        return [
            # On first call replication group is in available state, this will allow to initiate a delete
            # A replication group can only be deleted when it is in `available` state
            {"ReplicationGroups": [{"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "available"}]},
            # On second call replication group is in deleting state
            {"ReplicationGroups": [{"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "deleting"}]},
            # On further calls we will assume the replication group is deleted
            self._raise_replication_group_not_found_exp(mock_conn),
        ]

    def test_wait_for_deletion(self, mock_conn):
        mock_conn.describe_replication_groups.side_effect = self._mock_describe_side_effect(mock_conn)
        mock_conn.delete_replication_group.return_value = {
            "ReplicationGroup": {"ReplicationGroupId": REPLICATION_GROUP_ID}
        }

        response, deleted = ElastiCacheReplicationGroupHook().wait_for_deletion(
            replication_group_id=REPLICATION_GROUP_ID,
            # Initial call - status is `available`
            # 1st retry - status is `deleting`
            # 2nd retry - Replication group is deleted
            max_retries=2,
            initial_sleep_time=1,
        )
        assert response["ReplicationGroup"]["ReplicationGroupId"] == REPLICATION_GROUP_ID
        assert deleted is True

    def test_ensure_delete_replication_group_success(self, mock_conn):
        mock_conn.describe_replication_groups.side_effect = self._mock_describe_side_effect(mock_conn)
        mock_conn.delete_replication_group.return_value = {
            "ReplicationGroup": {"ReplicationGroupId": REPLICATION_GROUP_ID}
        }

        response = ElastiCacheReplicationGroupHook().ensure_delete_replication_group(
            replication_group_id=REPLICATION_GROUP_ID, initial_sleep_time=1, max_retries=2
        )

        assert response["ReplicationGroup"]["ReplicationGroupId"] == REPLICATION_GROUP_ID

    def test_ensure_delete_replication_group_failure(self, mock_conn):
        mock_conn.create_replication_group.return_value = {
            "ReplicationGroup": {"ReplicationGroupId": REPLICATION_GROUP_ID, "Status": "creating"}
        }
        mock_conn.describe_replication_groups.side_effect = self._mock_describe_side_effect(mock_conn)
        mock_conn.delete_replication_group.return_value = {
            "ReplicationGroup": {"ReplicationGroupId": REPLICATION_GROUP_ID}
        }

        with pytest.raises(AirflowException):
            # Try only 1 once with 1 sec buffer time. This will ensure that the `wait_for_deletion` loop
            # breaks quickly before the group is deleted and we get the Airflow exception
            ElastiCacheReplicationGroupHook().ensure_delete_replication_group(
                replication_group_id=REPLICATION_GROUP_ID, initial_sleep_time=1, max_retries=1
            )
