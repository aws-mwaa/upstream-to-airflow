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

import os
from collections import namedtuple
from typing import Any, Callable, Dict, List

import yaml

CommandType = List[str]
ExecutorConfigFunctionType = Callable[[CommandType], dict]
EcsQueuedTask = namedtuple("EcsQueuedTask", ("key", "command", "queue", "executor_config"))
ExecutorConfigType = Dict[str, Any]
EcsTaskInfo = namedtuple("EcsTaskInfo", ("cmd", "queue", "config"))

CONFIG_GROUP_NAME = "aws_ecs_executor"


def get_config_defaults():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    config_filename = curr_dir.replace("executors/ecs", "config_templates/config.yml")

    with open(config_filename) as config:
        options = yaml.safe_load(config)[CONFIG_GROUP_NAME]["options"]
        file_defaults = {
            option: default for (option, value) in options.items() if (default := value.get("default"))
        }

    return file_defaults


class EcsConfigKeys:
    """Keys loaded into the config which are related to the ECS Executor."""

    ASSIGN_PUBLIC_IP = "assign_public_ip"
    CLUSTER = "cluster"
    CONTAINER_NAME = "container_name"
    LAUNCH_TYPE = "launch_type"
    PLATFORM_VERSION = "platform_version"
    REGION = "region"
    RUN_TASK_KWARGS = "run_task_kwargs"
    SECURITY_GROUPS = "security_groups"
    SUBNETS = "subnets"
    TASK_DEFINITION = "task_definition"


class EcsExecutorException(Exception):
    """Thrown when something unexpected has occurred within the ECS ecosystem."""
