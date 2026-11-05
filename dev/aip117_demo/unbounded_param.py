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
"""Copy into files/dags to show the parse-time check: a free-text param cannot choose a pool."""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Lookup, Param

with DAG(
    "aip117_unbounded_param",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    params={"pool": Param("wh_small", type="string")},
    tags=["aip-117"],
):
    BashOperator(task_id="t", bash_command="true", pool=Lookup.param("pool"))
