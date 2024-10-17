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

from unittest.mock import Mock, patch

import pytest

from airflow.api_fastapi.core_api.app import (
    get_auth_backends,
    get_auth_manager,
    get_auth_manager_cls,
    init_auth_backends,
    init_auth_manager,
)
from airflow.auth.managers.simple.simple_auth_manager import SimpleAuthManager

from tests_common.test_utils.config import conf_vars


class TestApp:
    def test_get_auth_manager_cls(self):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
            }
        ):
            result = get_auth_manager_cls()
            assert result == SimpleAuthManager

    @patch("airflow.api_fastapi.core_api.app.init_auth_manager")
    def test_get_auth_manager_not_init(self, mock_init_auth_manager):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
            }
        ):
            auth_manager = Mock()
            mock_init_auth_manager.return_value = auth_manager
            result = get_auth_manager()
            mock_init_auth_manager.assert_called_once()
            assert result == auth_manager

    def test_get_auth_manager_init(self):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
            }
        ):
            init_auth_manager()
            result = get_auth_manager()
            assert isinstance(result, SimpleAuthManager)

    def test_get_auth_backends_not_init(self):
        with pytest.raises(RuntimeError, match="Auth backends have not been initialized yet"):
            get_auth_backends()

    def test_get_auth_backends_init(self):
        with conf_vars(
            {
                (
                    "api",
                    "auth_backends",
                ): "airflow.api.auth.backend.default",
            }
        ):
            init_auth_backends()
            results = get_auth_backends()
            assert len(results) == 1
            assert results[0].__name__ == "airflow.api.auth.backend.default"
