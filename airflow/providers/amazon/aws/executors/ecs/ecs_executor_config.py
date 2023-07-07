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

"""
Default AWS ECS Executor configuration.

This is the default configuration for calling the ECS `run_task` function.
The AWS ECS Executor calls Boto3's run_task(**kwargs) function with the kwargs templated by this
dictionary. See the URL below for documentation on the parameters accepted by the Boto3 run_task
function. In other words, if you don't like the way Airflow calls the Boto3 RunTask API, then
send your own kwargs by overriding the airflow config file.

.. seealso::
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
:return: Dictionary kwargs to be used by ECS run_task() function.
"""

from __future__ import annotations

import json
import warnings

from airflow.configuration import conf
from airflow.providers.amazon.aws.executors.ecs import CONFIG_GROUP_NAME

base_run_task_kwargs = conf.get(CONFIG_GROUP_NAME, "run_task_kwargs", fallback="")
ECS_EXECUTOR_RUN_TASK_KWARGS = json.loads(str(base_run_task_kwargs))

if conf.has_option(CONFIG_GROUP_NAME, "region"):
    ECS_EXECUTOR_RUN_TASK_KWARGS = {
        "cluster": conf.get(CONFIG_GROUP_NAME, "cluster"),
        "taskDefinition": conf.get(CONFIG_GROUP_NAME, "task_definition"),
        "platformVersion": conf.get(CONFIG_GROUP_NAME, "platform_version"),
        "overrides": {
            "containerOverrides": [
                {
                    "name": conf.get(CONFIG_GROUP_NAME, "container_name"),
                    # The executor will overwrite the 'command' property during execution.
                    # Must always be the first container!
                    "command": [],
                }
            ]
        },
        "count": 1,
    }

    if launch_type := conf.get(CONFIG_GROUP_NAME, "launch_type", fallback=False):
        ECS_EXECUTOR_RUN_TASK_KWARGS["launchType"] = launch_type

    # Only build this section if 'subnets', 'security_groups', and 'assign_public_ip' are all populated.
    if all(
        [
            subnets := conf.get(CONFIG_GROUP_NAME, "subnets", fallback=False),
            security_groups := conf.get(CONFIG_GROUP_NAME, "security_groups", fallback=False),
            assign_public_ip := conf.get(CONFIG_GROUP_NAME, "assign_public_ip", fallback=False),
        ]
    ):
        ECS_EXECUTOR_RUN_TASK_KWARGS["networkConfiguration"] = {
            "awsvpcConfiguration": {
                "subnets": str(subnets).split(","),
                "securityGroups": str(security_groups).split(","),
                "assignPublicIp": assign_public_ip,
            }
        }
    else:
        warnings.warn("`subnets`, `security_groups` and `assignPublicIp` are only used if all are defined.")
