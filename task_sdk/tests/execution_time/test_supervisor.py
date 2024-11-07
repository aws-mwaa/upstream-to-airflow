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
import os
import sys

import pytest
import structlog
import structlog.testing

from airflow.sdk.execution_time.supervisor import WatchedSubprocess
from airflow.utils import timezone as tz


def subprocess_main():
    # This is run in the subprocess!
    print("I'm a short message")
    sys.stdout.write("Message ")
    sys.stdout.write("split across two writes\n")
    sys.stdout.flush()

    import logging

    logging.getLogger("airflow.foobar").error("An error message")
    ...


@pytest.fixture
def disable_capturing(capsys):
    with capsys.disabled():
        yield


class TestWatchedSubprocess:
    @pytest.mark.usefixtures("disable_capturing")
    def test_reading_from_pipes(self, captured_logs, time_machine):
        # Ignore anything lower than INFO for this test. Captured_logs resets things for us afterwards
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))

        instant = tz.datetime(2024, 11, 7, 12, 34, 56, 78901)
        time_machine.move_to(instant, tick=False)

        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti={
                "id": "a",
                "task_id": "b",
                "try_number": 1,
            },
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == 0
        # TODO: how do we exclude debug logs?
        assert captured_logs == [
            {
                "chan": "stdout",
                "event": "I'm a short message",
                "level": "info",
                "logger": "task",
                "timestamp": "2024-11-07T12:34:56.078901Z",
            },
            {
                "chan": "stdout",
                "event": "Message split across two writes",
                "level": "info",
                "logger": "task",
                "timestamp": "2024-11-07T12:34:56.078901Z",
            },
            {"event": "An error message", "level": "error", "logger": "airflow.foobar", "timestamp": instant},
        ]
