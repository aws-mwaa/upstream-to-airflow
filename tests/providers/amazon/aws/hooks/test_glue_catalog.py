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

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_glue

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook

DB_NAME = "db"
TABLE_NAME = "table"
TABLE_INPUT = {
    "Name": TABLE_NAME,
    "StorageDescriptor": {
        "Columns": [{"Name": "string", "Type": "string", "Comment": "string"}],
        "Location": f"s3://mybucket/{DB_NAME}/{TABLE_NAME}",
    },
}
PARTITION_INPUT: dict = {
    "Values": [],
}


@pytest.fixture
def mock_conn():
    with mock.patch.object(GlueCatalogHook, "get_conn") as _get_conn:
        yield _get_conn.return_value


@pytest.fixture
def client():
    return boto3.client("glue")


@mock_glue
class TestGlueCatalogHook:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = GlueCatalogHook()

        assert hook.get_conn() is not None

    def test_conn_id(self):
        conn_id = "my_aws_conn_id"

        hook = GlueCatalogHook(aws_conn_id=conn_id)

        assert hook.aws_conn_id == conn_id

    def test_region(self):
        region = "us-west-2"

        hook = GlueCatalogHook(region_name=region)

        assert hook.region_name == region

    def test_get_partitions_empty(self, mock_conn):
        mock_conn.get_paginator.paginate.return_value = set()

        hook = GlueCatalogHook(region_name="us-east-1")

        assert hook.get_partitions("db", "tbl") == set()

    def test_get_partitions(self, mock_conn):
        response = [{"Partitions": [{"Values": ["2015-01-01"]}]}]
        mock_paginator = mock.Mock()
        mock_paginator.paginate.return_value = response
        mock_conn.get_paginator.return_value = mock_paginator

        result = GlueCatalogHook().get_partitions("db", "tbl", expression="foo=bar", page_size=2, max_items=3)

        assert result == {("2015-01-01",)}
        mock_conn.get_paginator.assert_called_once_with("get_partitions")
        mock_paginator.paginate.assert_called_once_with(
            DatabaseName="db",
            TableName="tbl",
            Expression="foo=bar",
            PaginationConfig={"PageSize": 2, "MaxItems": 3},
        )

    @mock.patch.object(GlueCatalogHook, "get_partitions")
    def test_check_for_partition(self, mock_get_partitions):
        mock_get_partitions.return_value = {("2018-01-01",)}

        assert GlueCatalogHook().check_for_partition("db", "tbl", "expr")
        mock_get_partitions.assert_called_once_with("db", "tbl", "expr", max_items=1)

    @mock.patch.object(GlueCatalogHook, "get_partitions")
    def test_check_for_partition_false(self, mock_get_partitions):
        mock_get_partitions.return_value = set()

        assert not GlueCatalogHook().check_for_partition("db", "tbl", "expr")

    def test_get_table_exists(self, client):
        client.create_database(DatabaseInput={"Name": DB_NAME})
        client.create_table(DatabaseName=DB_NAME, TableInput=TABLE_INPUT)

        result = GlueCatalogHook().get_table(DB_NAME, TABLE_NAME)

        assert result["Name"] == TABLE_INPUT["Name"]
        assert result["StorageDescriptor"]["Location"] == TABLE_INPUT["StorageDescriptor"]["Location"]

    def test_get_table_not_exists(self, client):
        client.create_database(DatabaseInput={"Name": DB_NAME})
        client.create_table(DatabaseName=DB_NAME, TableInput=TABLE_INPUT)

        with pytest.raises(Exception):
            GlueCatalogHook().get_table(DB_NAME, "dummy_table")

    def test_get_table_location(self, client):
        client.create_database(DatabaseInput={"Name": DB_NAME})
        client.create_table(DatabaseName=DB_NAME, TableInput=TABLE_INPUT)

        result = GlueCatalogHook().get_table_location(DB_NAME, TABLE_NAME)
        assert result == TABLE_INPUT["StorageDescriptor"]["Location"]

    def test_get_partition(self, client):
        client.create_database(DatabaseInput={"Name": DB_NAME})
        client.create_table(DatabaseName=DB_NAME, TableInput=TABLE_INPUT)
        client.create_partition(DatabaseName=DB_NAME, TableName=TABLE_NAME, PartitionInput=PARTITION_INPUT)

        result = GlueCatalogHook().get_partition(DB_NAME, TABLE_NAME, PARTITION_INPUT["Values"])

        assert result["Values"] == PARTITION_INPUT["Values"]
        assert result["DatabaseName"] == DB_NAME
        assert result["TableName"] == TABLE_INPUT["Name"]

    def test_get_partition_with_client_error(self, mock_conn):
        mock_conn.get_partition.side_effect = ClientError({}, "get_partition")

        with pytest.raises(AirflowException):
            GlueCatalogHook().get_partition(DB_NAME, TABLE_NAME, PARTITION_INPUT["Values"])

        mock_conn.get_partition.assert_called_once_with(
            DatabaseName=DB_NAME, TableName=TABLE_NAME, PartitionValues=PARTITION_INPUT["Values"]
        )

    def test_create_partition(self, client):
        client.create_database(DatabaseInput={"Name": DB_NAME})
        client.create_table(DatabaseName=DB_NAME, TableInput=TABLE_INPUT)

        result = GlueCatalogHook().create_partition(DB_NAME, TABLE_NAME, PARTITION_INPUT)

        assert result

    def test_create_partition_with_client_error(self, mock_conn):
        mock_conn.create_partition.side_effect = ClientError({}, "create_partition")

        with pytest.raises(AirflowException):
            GlueCatalogHook().create_partition(DB_NAME, TABLE_NAME, PARTITION_INPUT)

        mock_conn.create_partition.assert_called_once_with(
            DatabaseName=DB_NAME, TableName=TABLE_NAME, PartitionInput=PARTITION_INPUT
        )
