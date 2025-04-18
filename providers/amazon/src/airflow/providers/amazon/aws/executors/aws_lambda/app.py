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
import logging
import os
import subprocess

import boto3

"""
Example Lambda function to execute an Airflow command or workload. Use or modify this code as needed.
"""

log = logging.getLogger()
log.setLevel(logging.INFO)

# Create an SQS client. Credentials and region are automatically picked up from the environment.
sqs_client = boto3.client("sqs")

# Get the SQS queue URL from the environment variable.
QUEUE_URL = os.environ.get("QUEUE_URL")
TASK_KEY_KEY = "task_key"
COMMAND_KEY = "command"
RETURN_CODE_KEY = "return_code"


def lambda_handler(event, context):
    log.info("Received event: %s", event)
    log.info("Received context: %s", context)

    command = event.get(COMMAND_KEY)
    task_key = event.get(TASK_KEY_KEY)

    # Any pre-processing or validation of the command or use of the context can be done here.

    # This function must be called, it executes the Airflow command and reports to SQS.
    run_and_report(command, task_key)

    # Any post-processing or cleanup can be done here.


def run_and_report(command, task_key):
    """Execute the provided Airflow command or workload and report the result via SQS."""
    try:
        log.info("Starting execution for task: %s", task_key)
        result = subprocess.run(
            command, shell=isinstance(command, str), capture_output=True, stderr=subprocess.STDOUT
        )
        return_code = result.returncode
        log.info("Execution completed for task %s with return code %s", task_key, return_code)
        log.info("Output: %s", result.stdout.decode())
    except Exception as e:
        log.error("Error executing task %s: %s", task_key, e)
        return_code = 1  # Non-zero indicates failure to run the task

    if QUEUE_URL:
        message = json.dumps({TASK_KEY_KEY: task_key, RETURN_CODE_KEY: return_code})
        try:
            sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody=message)
            log.info("Sent result to SQS %s", message)
        except Exception as e:
            log.error("Failed to send message to SQS for task %s: %s", task_key, e)
    else:
        log.error("QUEUE_URL not provided in environment; unable to send task result.")
