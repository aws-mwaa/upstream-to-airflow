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

from socket import socketpair
from typing import TYPE_CHECKING

from airflow.sdk.execution_time.task_runner import CommsDecoder

if TYPE_CHECKING:
    from airflow.sdk.execution_time.comms import StartupDetails


class TestCommsDecoder:
    """Test the communication between the subprocess and the "supervisor"."""

    def test_recv_StartupDetails(self):
        r, w = socketpair()

        w.makefile("wb").write(
            b'{"type":"StartupDetails", "ti": {"id": "a", "task_id": "b", "try_number": 1}, '
            b'"file": "/dev/null", "requests_fd": 4'
            b"}\n"
        )

        decoder = CommsDecoder(input=r.makefile("r"))

        msg: StartupDetails = decoder.get_message()
        assert msg.ti.task_id == "b"
        assert msg.file == "/dev/null"

        # Since this was a StartupDetails message, the decoder should open the other socket
        assert decoder.request_socket.writable()
        assert decoder.request_socket.fileno() == 4
