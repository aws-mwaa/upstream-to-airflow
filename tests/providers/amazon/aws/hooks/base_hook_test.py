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

from airflow.providers_manager import ProvidersManager


class BaseHookUnitTest:
    @pytest.fixture
    def mock_conn(self, request):
        """request should contain a hook class object."""
        hook = request.param
        mock_path = f'{hook.__module__}.{hook.__qualname__}.conn'
        with mock.patch(mock_path, new_callable=mock.PropertyMock) as _mock_conn:
            yield _mock_conn
