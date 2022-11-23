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
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.hooks.sts import StsHook

AWS_ACCOUNT_ID = "123456789012"


MOCK_DATA = {
    "DataSetId": "DemoDataSet",
    "IngestionId": "DemoDataSet_Ingestion",
    "IngestionType": "INCREMENTAL_REFRESH",
    "AwsAccountId": AWS_ACCOUNT_ID,
}

MOCK_CREATE_INGESTION_RESPONSE = {
    "Status": 201,
    "Arn": "arn:aws:quicksight:us-east-1:123456789012:dataset/DemoDataSet/ingestion/DemoDataSet3_Ingestion",
    "IngestionId": "DemoDataSet_Ingestion",
    "IngestionStatus": "INITIALIZED",
    "RequestId": "fc1f7eea-1327-41d6-9af7-c12f097ed343",
}

MOCK_DESCRIBE_INGESTION_SUCCESS = {
    "Status": 200,
    "Ingestion": {
        "Arn": "arn:aws:quicksight:region:123456789012:dataset/DemoDataSet/ingestion/DemoDataSet3_Ingestion",
        "IngestionId": "DemoDataSet_Ingestion",
        "IngestionStatus": "COMPLETED",
        "ErrorInfo": {},
        "RowInfo": {"RowsIngested": 228, "RowsDropped": 0, "TotalRowsInDataset": 228},
        "CreatedTime": 1646589017.05,
        "IngestionTimeInSeconds": 17,
        "IngestionSizeInBytes": 27921,
        "RequestSource": "MANUAL",
        "RequestType": "FULL_REFRESH",
    },
    "RequestId": "DemoDataSet_Ingestion_Request_ID",
}

MOCK_DESCRIBE_INGESTION_FAILURE = {
    "Status": 403,
    "Ingestion": {
        "Arn": "arn:aws:quicksight:region:123456789012:dataset/DemoDataSet/ingestion/DemoDataSet3_Ingestion",
        "IngestionId": "DemoDataSet_Ingestion",
        "IngestionStatus": "Failed",
        "ErrorInfo": {},
        "RowInfo": {"RowsIngested": 228, "RowsDropped": 0, "TotalRowsInDataset": 228},
        "CreatedTime": 1646589017.05,
        "IngestionTimeInSeconds": 17,
        "IngestionSizeInBytes": 27921,
        "RequestSource": "MANUAL",
        "RequestType": "FULL_REFRESH",
    },
    "RequestId": "DemoDataSet_Ingestion_Request_ID",
}


@pytest.fixture
def mock_conn():
    with mock.patch.object(QuickSightHook, "get_conn") as _get_conn:
        yield _get_conn.return_value


class TestQuicksight:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = QuickSightHook()
        assert hook.conn is not None

    @mock.patch.object(StsHook, "get_account_number", return_value=AWS_ACCOUNT_ID)
    def test_create_ingestion(self, _, mock_conn):
        mock_conn.create_ingestion.return_value = MOCK_CREATE_INGESTION_RESPONSE
        expected_call_params = MOCK_DATA

        result = QuickSightHook().create_ingestion(
            data_set_id="DemoDataSet",
            ingestion_id="DemoDataSet_Ingestion",
            ingestion_type="INCREMENTAL_REFRESH",
        )

        mock_conn.create_ingestion.assert_called_with(**expected_call_params)
        assert result == MOCK_CREATE_INGESTION_RESPONSE

    @mock.patch.object(StsHook, "get_account_number", return_value=AWS_ACCOUNT_ID)
    def test_create_ingestion_exception(self, _):
        with pytest.raises(ClientError) as raised_exception:
            QuickSightHook().create_ingestion(
                data_set_id="DemoDataSet",
                ingestion_id="DemoDataSet_Ingestion",
                ingestion_type="INCREMENTAL_REFRESH",
            )
        ex = raised_exception.value

        assert ex.operation_name == "CreateIngestion"

    def test_get_job_status(self, mock_conn):
        mock_conn.describe_ingestion.return_value = MOCK_DESCRIBE_INGESTION_SUCCESS

        result = QuickSightHook().get_status(
            data_set_id="DemoDataSet",
            ingestion_id="DemoDataSet_Ingestion",
            aws_account_id="123456789012",
        )

        assert result == "COMPLETED"

    def test_get_job_status_failed(self, mock_conn):
        mock_conn.describe_ingestion.return_value = MOCK_DESCRIBE_INGESTION_FAILURE

        result = QuickSightHook().get_status(
            data_set_id="DemoDataSet",
            ingestion_id="DemoDataSet_Ingestion",
            aws_account_id="123456789012",
        )

        assert result == "Failed"
