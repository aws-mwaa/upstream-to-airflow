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
from typing import Callable

from fastapi import HTTPException
from starlette.requests import Request

from airflow.api_fastapi.core_api.app import get_auth_backends, get_auth_manager
from airflow.auth.managers.base_auth_manager import ResourceMethod
from airflow.auth.managers.models.resource_details import DagAccessEntity, DagDetails

log = logging.getLogger(__name__)


def set_user_from_auth_backends(request: Request) -> None:
    auth_backends = get_auth_backends()
    user = None
    for auth_backend in auth_backends:
        user = auth_backend.get_user()
        if user is not None:
            break
    request.state.user = user


def requires_access_dag(method: ResourceMethod, access_entity: DagAccessEntity | None = None) -> Callable:
    def inner(
        request: Request,
        dag_id: str | None = None,
    ) -> None:
        def callback():
            return get_auth_manager().is_authorized_dag(
                method=method,
                access_entity=access_entity,
                details=DagDetails(id=dag_id),
                user=request.state.user,
            )

        _requires_access(
            is_authorized_callback=callback,
        )

    return inner


def _requires_access(
    *,
    is_authorized_callback: Callable[[], bool],
) -> None:
    if not is_authorized_callback():
        raise HTTPException(403, "Forbidden")
