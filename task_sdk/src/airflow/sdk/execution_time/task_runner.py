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
"""The entrypoint for the actual task execution process."""

from __future__ import annotations

import os
import sys
from io import FileIO
from typing import TYPE_CHECKING, TextIO

import attrs
import msgspec
import structlog

from airflow.sdk import BaseOperator
from airflow.sdk.execution_time.comms import StartupDetails, TaskInstance, ToSupervisor, ToTask
from airflow.sdk.log import configure_logging

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger


class RuntimeTaskInstance(TaskInstance, kw_only=True):
    task: BaseOperator


def parse(what: StartupDetails) -> RuntimeTaskInstance:
    # TODO: Task-SDK:
    # Using DagBag here is aoubt 98% wrong, but it'll do for now

    from airflow.models.dagbag import DagBag

    bag = DagBag(
        dag_folder=what.file,
        include_examples=False,
        safe_mode=False,
        load_op_links=False,
    )
    if TYPE_CHECKING:
        assert what.ti.run
        assert what.ti.run.dag_id

    dag = bag.dags[what.ti.run.dag_id]

    # install_loader()

    # TODO: Handle task not found
    task = dag.task_dict[what.ti.task_id]
    if not isinstance(task, BaseOperator):
        raise TypeError(f"task is of the wrong type, got {type(task)}, wanted {BaseOperator}")
    return RuntimeTaskInstance(**msgspec.structs.asdict(what.ti), task=task)


@attrs.define()
class CommsDecoder:
    """Handle communication between the task in this process and the supervisor parent process."""

    input: TextIO = sys.stdin

    decoder: msgspec.json.Decoder[ToTask] = attrs.field(factory=lambda: msgspec.json.Decoder(type=ToTask))
    encoder: msgspec.json.Encoder = attrs.field(factory=msgspec.json.Encoder)

    request_socket: FileIO = attrs.field(init=False, default=None)

    # Allocate a buffer for en/decoding. We use a small non-empty buffer to avoid reallocating for small messages.
    buffer: bytearray = attrs.field(factory=lambda: bytearray(64))

    def get_message(self) -> ToTask:
        """
        Get a message from the parent.

        This will block until the message has been received.
        """
        line = self.input.readline()
        try:
            msg = self.decoder.decode(line)
        except Exception:
            structlog.get_logger(logger_name="CommsDecoder").exception("Unable to decode message", line=line)
            raise

        if isinstance(msg, StartupDetails):
            # If we read a startup message, pull out the FDs we care about!
            if msg.requests_fd > 0:
                self.request_socket = os.fdopen(msg.requests_fd, "wb", buffering=0)
        return msg

    def send_request(self, log: Logger, msg: ToSupervisor):
        buffer = self.buffer
        self.encoder.encode_into(msg, buffer)
        buffer += b"\n"

        log.debug("Sending request", json=buffer)
        self.request_socket.write(buffer)


# This global variable will be used by Connection/Variable classes etc to send requests to
SUPERVISOR_COMMS: CommsDecoder

# State machine!
# 1. Start up (receive details from supervisor)
# 2. Execution (run task code, possibly send requests)
# 3. Shutdown and report status


def startup() -> tuple[RuntimeTaskInstance, Logger]:
    msg = SUPERVISOR_COMMS.get_message()

    if isinstance(msg, StartupDetails):
        log = structlog.get_logger(logger_name="task")
        # TODO: set the "magic loop" context vars for parsing
        ti = parse(msg)
        log.debug("DAG file parsed", file=msg.file)
        return ti, log
    else:
        raise RuntimeError(f"Unhandled  startup message {type(msg)} {msg}")

    # TODO: Render fields here


def run(ti: RuntimeTaskInstance, log: Logger):
    """Run the task in this process."""
    from airflow.exceptions import (
        AirflowException,
        AirflowFailException,
        AirflowRescheduleException,
        AirflowSensorTimeout,
        AirflowSkipException,
        AirflowTaskTerminated,
        AirflowTaskTimeout,
        TaskDeferred,
    )

    if TYPE_CHECKING:
        assert ti.task is not None
        assert isinstance(ti.task, BaseOperator)
    try:
        # TODO: pre execute etc.
        # TODO next_method to support resuming from deferred
        # TODO: Get a real context object
        ti.task.execute({"task_instance": ti})  # type: ignore[attr-defined]
    except TaskDeferred:
        ...
    except AirflowSkipException:
        ...
    except AirflowRescheduleException:
        ...
    except (AirflowFailException, AirflowSensorTimeout):
        # If AirflowFailException is raised, task should not retry.
        ...
    except (AirflowTaskTimeout, AirflowException, AirflowTaskTerminated):
        ...
    except SystemExit:
        ...
    except BaseException:
        ...


def finalize(log: Logger): ...


def main():
    # Configure logs to be JSON, so that we can pass it to the parent process
    # Don't cache this log though!

    configure_logging(enable_pretty_log=False)

    # TODO: add an exception here, it causes an oof of a stack trace!

    global SUPERVISOR_COMMS
    SUPERVISOR_COMMS = CommsDecoder()
    try:
        ti, log = startup()
        run(ti, log)
        finalize(log)
    except KeyboardInterrupt:
        log = structlog.get_logger(logger_name="task")
        log.exception("Ctrl-c hit")
        exit(2)
    except Exception:
        log = structlog.get_logger(logger_name="task")
        log.exception("Top level error")
        exit(1)


if __name__ == "__main__":
    main()
