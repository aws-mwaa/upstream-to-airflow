#
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

import importlib

import pytest

from airflow.sdk._shared.observability.metrics import stats as shared_stats
from airflow.sdk._shared.observability.metrics.base_stats_logger import NoStatsLogger
from airflow.sdk._shared.observability.metrics.statsd_logger import SafeStatsdLogger
from airflow.sdk.observability import stats

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def stats_not_set_up(monkeypatch):
    """A process that never set up the Task SDK's stats module, like the scheduler."""
    monkeypatch.setattr(shared_stats, "_factory", None)
    monkeypatch.setattr(shared_stats, "_backend", None)
    monkeypatch.setattr(shared_stats, "_export_legacy_names", True)


@conf_vars({("metrics", "statsd_on"): "True", ("metrics", "legacy_names_on"): "False"})
def test_import_sets_up_stats_from_config(stats_not_set_up):
    importlib.reload(stats)

    assert isinstance(shared_stats._get_backend(), SafeStatsdLogger)
    assert shared_stats._export_legacy_names is False


@conf_vars({("metrics", "statsd_on"): "True"})
def test_import_keeps_an_earlier_setup(stats_not_set_up):
    stats.initialize(factory=NoStatsLogger, export_legacy_names=True)

    importlib.reload(stats)

    assert isinstance(shared_stats._get_backend(), NoStatsLogger)
