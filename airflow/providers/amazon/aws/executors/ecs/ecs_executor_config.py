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
# TODO  this is no longer the default config, fix this doctring
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
from json import JSONDecodeError

from airflow.configuration import conf
from airflow.providers.amazon.aws.executors.ecs.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    EcsConfigKeys,
)
from airflow.utils.helpers import prune_dict


def _flatten_dict(nested_dict):
    """
    Recursively unpack a nested dict and return it as a flat dict.

    For example, _flatten_dict({'a': 'a', 'b': 'b', 'c': {'d': 'd'}}) returns {'a': 'a', 'b': 'b', 'd': 'd'}.
    """
    items = []
    for key, value in nested_dict.items():
        if isinstance(value, dict):
            items.extend(_flatten_dict(value).items())
        else:
            items.append((key, value))
    return dict(items)


def _load_default_kwargs() -> dict[str, str]:
    return CONFIG_DEFAULTS


def _load_template_kwargs() -> dict[str, str]:
    run_task_kwargs_value = conf.get(CONFIG_GROUP_NAME, EcsConfigKeys.RUN_TASK_KWARGS, fallback=dict())
    task_kwargs = json.loads(str(run_task_kwargs_value))
    return _flatten_dict(task_kwargs)


def _load_provided_kwargs() -> dict[str, str]:
    return prune_dict(
        {key: conf.get(CONFIG_GROUP_NAME, key, fallback=None) for key in EcsConfigKeys()}
    )


def _build_task_kwargs() -> dict:
    settings = dict()
    settings.update(_load_default_kwargs())
    if not conf.has_section(CONFIG_GROUP_NAME):
        return settings

    settings.update(_load_template_kwargs())
    settings.update(_load_provided_kwargs())

    task_kwargs = {
        "cluster": settings.get(EcsConfigKeys.CLUSTER),
        "taskDefinition": settings.get(EcsConfigKeys.TASK_DEFINITION),
        "platformVersion": settings.get(EcsConfigKeys.PLATFORM_VERSION),
        "overrides": {
            "containerOverrides": [
                {
                    "name": settings.get(EcsConfigKeys.CONTAINER_NAME),
                    # The executor will overwrite the 'command' property during execution.
                    # Must always be the first container!
                    "command": [],
                }
            ]
        },
        "count": 1,
        "launchType": settings.get(EcsConfigKeys.LAUNCH_TYPE),
    }

    if any(
        [
            subnets := settings.get(EcsConfigKeys.SUBNETS),
            security_groups := settings.get(EcsConfigKeys.SECURITY_GROUPS),
            assign_public_ip := settings.get(EcsConfigKeys.ASSIGN_PUBLIC_IP),
        ]
    ):
        network_config = prune_dict(
            {
                "awsvpcConfiguration": {
                    "subnets": str(subnets).split(",") if subnets else subnets,
                    "securityGroups": str(security_groups).split(",") if security_groups else security_groups,
                    "assignPublicIp": "ENABLED" if bool(assign_public_ip) else "DISABLED",
                }
            }
        )

        if "subnets" not in network_config["awsvpcConfiguration"]:
            raise ValueError("At least one subnet is required to run a task.")

        task_kwargs["networkConfiguration"] = network_config

    try:
        json.loads(json.dumps(task_kwargs))
    except JSONDecodeError:
        raise ValueError(
            f"AWS ECS Executor config values must be JSON serializable. Got {task_kwargs}"
        )

    return task_kwargs


ECS_EXECUTOR_RUN_TASK_KWARGS = _build_task_kwargs()
