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

import json

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class BedrockHook(AwsBaseHook):
    """
    Interact with Amazon Bedrock.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock") <Bedrock.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)


class BedrockRuntimeHook(AwsBaseHook):
    """
    Interact with the Amazon Bedrock Runtime.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-runtime") <BedrockRuntime.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-runtime"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)

    def suggest_code_improvements(self, code: str, max_tokens: int = 2000) -> str:
        self.log.info("Ask bedrock to optimize this code: %s", code)

        body = {
            "messages": [
                {
                    "role": "user",
                    "content": f"Your task is to analyze the provided Apache Airflow DAG code snippet and "
                               f"generate an optimized version of it. Identify areas where the code can be "
                               f"made more efficient, faster, or less resource-intensive. "
                               f"The optimized code should maintain the same functionality as the original "
                               f"code while demonstrating improved efficiency and following Airflow best "
                               f"practices.\n"
                               f"When optimizing the DAG, consider the following best practices and "
                               f"features:\n"
                               f"1. Move top-level imports to task level: relocate imports, especially for "
                               f"heavy libraries like pandas or numpy, inside task functions to reduce "
                               f"DAG parsing time.\n"
                               f"2. Avoid top-level code execution: Move any heavy computations, database "
                               f"queries, or API calls inside task functions.\n"
                               f"3. Use dynamic task mapping (introduced in Airflow 2.3+) where appropriate "
                               f"to handle parallel tasks more efficiently.\n"
                               f"4. Combine tasks where possible to reduce overhead, especially for small, "
                               f"quick operations.\n"
                               f"5. Use the TaskFlow API with @task decorators for cleaner, more Pythonic "
                               f"DAG definitions.\n"
                               f"6. Set `catchup=False` unless historical runs are specifically needed.\n"
                               f"7. Use `max_active_runs=1` to prevent concurrent DAG runs if not required.\n"
                               f"8. Implement a watcher pattern using a task with "
                               f"`trigger_rule='one_failed'` to ensure proper DAG failure handling.\n"
                               f"9. Use Airflow Variables for configuration, but access them inside tasks "
                               f"to avoid slow DAG parsing.\n"
                               f"10. Use appropriate Airflow features like TaskGroups for organizing complex "
                               f"DAGs.\n"
                               f"11. Implement proper error handling, especially for external API calls or "
                               f"database operations.\n"
                               f"12. Use default_args for common task parameters.\n\n"
                               f"Please provide the optimized code wrapped in "
                               f"<optimized_code></optimized_code> XML tags. After the optimized code, "
                               f"include a brief explanation of the key optimizations made, wrapped in "
                               f"<optimization_explanation></optimization_explanation> tags.\n\n"
                               f"Here's the original code to optimize:\n"
                               f"<code>\n{code}\n</code>",
                }
            ],
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
        }
        response = self.conn.invoke_model(
            body=str.encode(json.dumps(body)),
            modelId="anthropic.claude-3-sonnet-20240229-v1:0"
        )
        body_json = json.loads(response['body'].read().decode('utf-8'))

        self.log.info("Response from Bedrock: %s", body_json["content"][0]["text"])

        return body_json["content"][0]["text"]


class BedrockAgentHook(AwsBaseHook):
    """
    Interact with the Amazon Agents for Bedrock API.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-agent") <AgentsforBedrock.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-agent"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)


class BedrockAgentRuntimeHook(AwsBaseHook):
    """
    Interact with the Amazon Agents for Bedrock API.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-agent-runtime") <AgentsforBedrockRuntime.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-agent-runtime"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)
