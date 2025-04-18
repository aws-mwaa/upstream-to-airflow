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

from collections.abc import Sequence
from typing import Any

from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys

# Move some or all of this to a shared aws executor utils eventually
CONFIG_GROUP_NAME = "aws_lambda_executor"

CONFIG_DEFAULTS = {
    "conn_id": "aws_default",
    "max_run_task_attempts": "3",
    "check_health_on_startup": "True",
}

INVALID_CREDENTIALS_EXCEPTIONS = [
    "ExpiredTokenException",
    "InvalidClientTokenId",
    "UnrecognizedClientException",
]


class InvokeLambdaKwargsConfigKeys(BaseConfigKeys):
    """Keys loaded into the config which are valid lambda invoke args."""

    FUNCTION_NAME = "function_name"
    QUALIFIER = "function_qualifier"


class AllLambdaConfigKeys(InvokeLambdaKwargsConfigKeys):
    """All keys loaded into the config which are related to the Lambda Executor."""

    AWS_CONN_ID = "conn_id"
    CHECK_HEALTH_ON_STARTUP = "check_health_on_startup"
    MAX_INVOKE_ATTEMPTS = "max_run_task_attempts"
    REGION_NAME = "region_name"
    SQS_QUEUE_URL = "sqs_queue_url"


CommandType = Sequence[str]
ExecutorConfigType = dict[str, Any]
