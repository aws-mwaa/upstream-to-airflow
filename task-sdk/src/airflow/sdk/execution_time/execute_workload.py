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

"""
Module for executing an Airflow task using the workload json provided by a input file.

Usage:
    python execute_workload.py <input_file>

Arguments:
    input_file (str): Path to the JSON file containing the workload definition.
"""

from __future__ import annotations

import argparse
import sys

import structlog

log = structlog.get_logger(logger_name=__name__)


def execute_workload(workload) -> None:
    # from pydantic import TypeAdapter

    from airflow.configuration import conf
    from airflow.executors import workloads
    from airflow.sdk.execution_time.supervisor import supervise
    from airflow.sdk.log import configure_logging
    from airflow.settings import dispose_orm

    dispose_orm(do_log=False)

    configure_logging(output=sys.stdout.buffer, enable_pretty_log=False)

    # decoder = TypeAdapter[workloads.All](workloads.All)
    # workload = decoder.validate_json(input)

    if not isinstance(workload, workloads.ExecuteTask):
        raise ValueError(f"We do not know how to handle {type(workload)}")

    log.info("Executing workload", workload=workload)
    server = conf.get("core", "execution_api_server_url")
    log.info("Using server", server=server)

    supervise(
        # This is the "wrong" ti type, but it duck types the same. TODO: Create a protocol for this.
        ti=workload.ti,  # type: ignore[arg-type]
        dag_rel_path=workload.dag_rel_path,
        bundle_info=workload.bundle_info,
        token=workload.token,
        server=server,
        log_path=workload.log_path,
        # Include the output of the task to stdout too, so that in process logs can be read from via the
        # kubeapi as pod logs.
        subprocess_logs_to_stdout=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Execute a workload in a Containerised executor using the task SDK."
    )

    parser.add_argument(
        "input_json",
        help="Path to the input JSON file containing the execution workload payload or the json string itself.",
    )

    args = parser.parse_args()

    from pydantic import TypeAdapter

    from airflow.executors import workloads

    decoder = TypeAdapter[workloads.All](workloads.All)
    try:
        with open(args.input_json) as file:
            input_data = file.read()
            workload = decoder.validate_json(input_data)
    except Exception as file_open_e:
        # We couldn't find that file, so let's assume it's a JSON string.
        try:
            workload = decoder.validate_json(args.input_json)
        except Exception as e:
            # At this point catch any exception and log
            log.error("Failed to parse input JSON as path", error=str(file_open_e))
            log.error("Failed to parse input JSON as string", error=str(e))
            log.error("input was neither valid json nor a path to a file containing valid input json")
            sys.exit(1)

    execute_workload(workload)


if __name__ == "__main__":
    main()
