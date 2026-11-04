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
"""Demo Dags for the Triggers page: asset watchers, deferred tasks, and a deadline callback."""

from __future__ import annotations

import datetime
import time

from demo_triggers import QueueWatcher, WaitForJobOperator

from airflow.sdk import DAG, Asset, AssetWatcher, AsyncCallback, DeadlineAlert, DeadlineReference, task

START = datetime.datetime(2026, 1, 1)


def watched(asset: str, queue: str) -> Asset:
    return Asset(asset, watchers=[AssetWatcher(name=queue, trigger=QueueWatcher(queue_name=queue))])


# Event-driven Dags: each asset is updated by a watcher on its queue.
consumers = {
    "process_orders": [watched("orders", "orders_queue")],
    "process_payments": [watched("payments", "payments_queue")],
    "refresh_inventory": [watched("inventory", "inventory_queue")],
    # One queue feeds two assets: the same trigger serves both watchers.
    "sessionize_clickstream": [watched("clickstream", "clickstream_queue")],
    "archive_clickstream": [watched("clickstream_archive", "clickstream_queue")],
}
for dag_id, schedule in consumers.items():
    with DAG(dag_id, schedule=schedule, start_date=START, catchup=False, tags=["event-driven"]):

        @task
        def consume():
            print("consumed")

        consume()

with DAG("example_deferred_task", schedule=None, start_date=START, catchup=False, tags=["deferrable"]):
    WaitForJobOperator(task_id="wait_for_trigger", job="nightly-export")
    WaitForJobOperator.partial(task_id="wait_for_partition").expand(
        job=["orders-us", "orders-eu", "orders-apac"]
    )

with DAG("example_partner_export", schedule=None, start_date=START, catchup=False, tags=["deferrable"]):
    WaitForJobOperator(task_id="wait_for_export", job="partner-export")

with DAG(
    "example_deadline_callback",
    schedule=None,
    start_date=START,
    catchup=False,
    tags=["deadlines"],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=datetime.timedelta(seconds=15),
        callback=AsyncCallback("demo_triggers.page_on_call", kwargs={"dag_id": "example_deadline_callback"}),
        name="warehouse_load_sla",
    ),
):

    @task
    def load_warehouse():
        time.sleep(240)

    load_warehouse()
