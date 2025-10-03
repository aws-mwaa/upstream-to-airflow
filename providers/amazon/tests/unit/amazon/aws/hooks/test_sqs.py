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
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.sqs import SqsHook

QUEUE_URL = "https://sqs.region.amazonaws.com/123456789/test-queue"
MESSAGE_BODY = "test message"
DELAY = 5
DEDUPE = "banana"
MSG_ATTRIBUTES = {
    "Author": {
        "StringValue": "test-user",
        "DataType": "String",
    },
    "Priority": {
        "StringValue": "1",
        "DataType": "Number",
    },
}

MESSAGE_ID_KEY = "MessageId"

SEND_MESSAGE_DEFAULTS = {
    "DelaySeconds": 0,
    "MessageAttributes": {},
}


class TestSqsHook:
    @mock_aws
    def test_get_conn(self):
        hook = SqsHook(aws_conn_id="aws_default")
        assert hook.get_conn() is not None


@pytest.mark.asyncio
class TestAsyncSqsHook:
    """The mock_aws decorator uses `moto` which does not currently support async SQS so we mock it manually."""

    @pytest.fixture
    def hook(self):
        return SqsHook(aws_conn_id="aws_default")

    @pytest.fixture
    def mock_async_client(self):
        mock_client = mock.AsyncMock()
        mock_client.send_message.return_value = {MESSAGE_ID_KEY: "test-message-id"}
        return mock_client

    @pytest.fixture
    def mock_get_async_conn(self, mock_async_client):
        with mock.patch.object(SqsHook, "get_async_conn") as mocked_conn:
            mocked_conn.return_value = mock_async_client
            mocked_conn.return_value.__aenter__.return_value = mock_async_client
            yield mocked_conn

    async def test_get_async_conn(self, hook, mock_get_async_conn, mock_async_client):
        # Test context manager access
        async with await hook.get_async_conn() as async_conn:
            assert async_conn is mock_async_client

        # Test direct access
        async_conn = await hook.get_async_conn()
        assert async_conn is mock_async_client

    async def test_asend_message_minimal(self, hook, mock_get_async_conn, mock_async_client):
        response = await hook.asend_message(queue_url=QUEUE_URL, message_body=MESSAGE_BODY)

        assert MESSAGE_ID_KEY in response
        mock_async_client.send_message.assert_called_once_with(
            MessageBody=MESSAGE_BODY, QueueUrl=QUEUE_URL, **SEND_MESSAGE_DEFAULTS
        )

    async def test_asend_message_with_attributes(self, hook, mock_get_async_conn, mock_async_client):
        response = await hook.asend_message(
            queue_url=QUEUE_URL,
            message_body=MESSAGE_BODY,
            message_attributes=MSG_ATTRIBUTES,
            delay_seconds=DELAY,
            message_deduplication_id=DEDUPE,
        )

        assert MESSAGE_ID_KEY in response
        mock_async_client.send_message.assert_called_once_with(
            DelaySeconds=DELAY,
            MessageBody=MESSAGE_BODY,
            MessageAttributes=MSG_ATTRIBUTES,
            QueueUrl=QUEUE_URL,
            MessageDeduplicationId=DEDUPE,
        )
