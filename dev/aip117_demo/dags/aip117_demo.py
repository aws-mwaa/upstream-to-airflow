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
"""AIP-117 demo Dags. See dev/aip117_demo/README.md."""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Lookup, Param, TaskGroup

REGIONS = ["us_east", "us_west", "eu_central", "eu_west", "ap_south", "ap_east"]

# 1. Choose capacity at trigger time. The trigger form shows the enum param as a dropdown.
with DAG(
    "aip117_regional_sync",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    params={
        "warehouse_pool": Param(
            "wh_small", enum=["wh_small", "wh_large"], description="Warehouse capacity for this run"
        ),
    },
    default_args={"pool": Lookup.param("warehouse_pool")},
    tags=["aip-117"],
):
    for region in REGIONS:
        with TaskGroup(region):
            for table in ("orders", "customers", "events"):
                BashOperator(task_id=f"load_{table}", bash_command="sleep 30")

# 2. Keep backfills from starving daily runs.
with DAG(
    "aip117_daily_load",
    schedule="@daily",
    start_date=datetime.datetime(2026, 10, 1),
    catchup=False,
    default_args={"pool": Lookup.run_type({"backfill": "backfill_pool"}, default="etl_pool")},
    tags=["aip-117"],
):
    for i in range(4):
        BashOperator(task_id=f"load_{i}", bash_command="sleep 15")

# 3. A Dag nobody prepared: no lookups, overridden at trigger time.
with DAG(
    "aip117_reprocess_orders",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    tags=["aip-117"],
):
    extract = BashOperator(task_id="extract", bash_command="sleep 10", pool="etl_pool")
    with TaskGroup("transform") as transform:
        for i in range(3):
            BashOperator(task_id=f"part_{i}", bash_command="sleep 10", pool="etl_pool")
    load = BashOperator(task_id="load", bash_command="sleep 10", pool="etl_pool")
    extract >> transform >> load
