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
"""Stats module - re-exports from airflow.sdk._shared.observability.metrics.stats."""

from __future__ import annotations

from airflow.sdk._shared.observability.metrics.stats import (
    Stats,
    decr,
    gauge,
    incr,
    initialize,
    is_initialized,
    normalize_name_for_stats,
    timer,
    timing,
)
from airflow.sdk.configuration import conf
from airflow.sdk.observability.metrics import stats_utils

__all__ = [
    "Stats",
    "decr",
    "gauge",
    "incr",
    "initialize",
    "normalize_name_for_stats",
    "timer",
    "timing",
]

# Providers send their metrics through this copy of the stats module, also from the scheduler, API server,
# triggerer and Dag processor, which only set up Airflow's own copy: without this, they would be dropped there.
if not is_initialized():
    initialize(
        factory=stats_utils.get_stats_factory(),
        export_legacy_names=conf.getboolean("metrics", "legacy_names_on"),
    )
