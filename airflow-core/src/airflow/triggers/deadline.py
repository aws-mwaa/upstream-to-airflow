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
import traceback
from collections.abc import AsyncIterator
from typing import Any

from airflow.callbacks.callback_requests import DagRunContext, DagCallbackRequest
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.module_loading import import_string, qualname

log = logging.getLogger(__name__)

PAYLOAD_STATUS_KEY = "state"
PAYLOAD_BODY_KEY = "body"


class DeadlineCallbackTrigger(BaseTrigger):
    """Trigger that executes a deadline callback function asynchronously."""

    # def __init__(self, callback_path: str, callback_kwargs: dict[str, Any] | None = None, request: DagCallbackRequest | None = None, context_from_server: DagRunContext | None = None, context = None):
    def __init__(self, callback_path: str, callback_kwargs: dict[str, Any] | None = None):
        super().__init__()
        self.callback_path = callback_path
        self.callback_kwargs = callback_kwargs or {}
        # self.request = request
        # self.context_from_server = context_from_server
        # self.context = {}

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            qualname(self),
            # {attr: getattr(self, attr) for attr in ("callback_path", "callback_kwargs", "request", "context")},
            {attr: getattr(self, attr) for attr in ("callback_path", "callback_kwargs")},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        from airflow.models.deadline import DeadlineCallbackState  # to avoid cyclic imports

        try:
            callback = import_string(self.callback_path)

            yield TriggerEvent({PAYLOAD_STATUS_KEY: DeadlineCallbackState.RUNNING})
            # result = await callback(**self.callback_kwargs | {"context": get_deadline_template_context(self.request)})
            result = await callback(**self.callback_kwargs)

            yield TriggerEvent({PAYLOAD_STATUS_KEY: DeadlineCallbackState.SUCCESS, PAYLOAD_BODY_KEY: result})
            log.info("Deadline callback completed with return value: %s", result)

        except Exception as e:
            if isinstance(e, ImportError):
                message = "Failed to import this deadline callback on the triggerer"
            elif isinstance(e, TypeError) and "await" in str(e):
                message = "Failed to run this deadline callback because it is not awaitable"
            else:
                message = "An error occurred during execution of this deadline callback"

            yield TriggerEvent(
                {
                    PAYLOAD_STATUS_KEY: DeadlineCallbackState.FAILED,
                    PAYLOAD_BODY_KEY: f"{message}: {traceback.format_exception(e)}",
                }
            )

            log.exception("%s: %s; kwargs: %s\n%s", message, self.callback_path, self.callback_kwargs, e)

# def get_deadline_template_context(req: DagCallbackRequest | None = None):
#     from airflow.models import DagBag
#     from airflow.sdk.api.datamodels._generated import TIRunContext
#     from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
#     from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance as TIDataModel
#
#     from airflow.serialization.serialized_objects import BaseSerialization
#
#     context_from_server = req.context_from_server
#     # context_from_server = BaseSerialization.deserialize(context_from_server)
#
#     log.warning(context_from_server)
#     log.warning(type(context_from_server))
#     if context_from_server is not None and context_from_server.last_ti is not None:
#         dag_bag = DagBag(collect_dags=False)
#         dag = dag_bag.get_dag(context_from_server.dag_run.dag_id)
#         task = dag.get_task(context_from_server.last_ti.task_id)
#
#         runtime_ti = RuntimeTaskInstance.model_construct(
#             **context_from_server.last_ti.model_dump(exclude_unset=True),
#             task=task,
#             _ti_context_from_server=TIRunContext.model_construct(
#                 dag_run=context_from_server.dag_run,
#                 max_tries=task.retries,
#             ),
#         )
#         context = runtime_ti.get_template_context()
#         context["reason"] = "deadline_miss"
#         log.warning(context)
#         return context
#     return {"f": "g"}
