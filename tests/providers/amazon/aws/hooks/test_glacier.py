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

import logging
from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.glacier import GlacierHook

CREDENTIALS = "aws_conn"
VAULT_NAME = "airflow"
JOB_ID = "1234abcd"
REQUEST_RESULT = {"jobId": "1234abcd"}
RESPONSE_BODY = {"body": "data"}
JOB_STATUS = {"Action": "", "StatusCode": "Succeeded"}


@pytest.fixture
def mock_conn():
    with mock.patch.object(GlacierHook, "get_conn") as _get_conn:
        yield _get_conn.return_value


class TestAmazonGlacierHook:
    def test_retrieve_inventory_should_return_job_id(self, mock_conn):
        job_id = {"jobId": "1234abcd"}
        mock_conn.initiate_job.return_value = job_id

        result = GlacierHook().retrieve_inventory(VAULT_NAME)

        assert job_id == result

    def test_retrieve_inventory_should_log_mgs(self, mock_conn, caplog):
        job_id = {"jobId": "1234abcd"}
        mock_conn.initiate_job.return_value = job_id
        expected_messages = [
            f"Retrieving inventory for vault: {VAULT_NAME}",
            f"Initiated inventory-retrieval job for: {VAULT_NAME}",
            f"Retrieval Job ID: {job_id.get('jobId')}",
        ]

        with caplog.at_level(logging.INFO):
            GlacierHook().retrieve_inventory(VAULT_NAME)
        logged_messages = caplog.messages

        assert len(logged_messages) == len(expected_messages)
        for counter, message in enumerate(logged_messages):
            assert expected_messages[counter] in message

    def test_retrieve_inventory_results_should_return_response(self, mock_conn):
        mock_conn.get_job_output.return_value = RESPONSE_BODY

        response = GlacierHook().retrieve_inventory_results(VAULT_NAME, JOB_ID)

        assert response == RESPONSE_BODY

    def test_retrieve_inventory_results_should_log_mgs(self, mock_conn, caplog):
        mock_conn.get_job_output.return_value = REQUEST_RESULT

        with caplog.at_level(logging.INFO):
            GlacierHook().retrieve_inventory_results(VAULT_NAME, JOB_ID)

        assert len(caplog.messages) == 1
        assert caplog.messages[0] == f"Retrieving the job results for vault: {VAULT_NAME}..."

    def test_describe_job_should_return_status_succeeded(self, mock_conn):
        mock_conn.describe_job.return_value = JOB_STATUS

        response = GlacierHook().describe_job(VAULT_NAME, JOB_ID)

        assert response == JOB_STATUS

    def test_describe_job_should_log_mgs(self, mock_conn, caplog):
        mock_conn.describe_job.return_value = JOB_STATUS
        expected_messages = [
            f"Retrieving status for vault: {VAULT_NAME} and job {JOB_ID}",
            f"Job status: {JOB_STATUS.get('Action')}, code status: {JOB_STATUS.get('StatusCode')}",
        ]

        with caplog.at_level(logging.INFO):
            GlacierHook().describe_job(VAULT_NAME, JOB_ID)

        assert len(caplog.messages) == len(expected_messages)
        for counter, message in enumerate(caplog.messages):
            assert expected_messages[counter] in message
