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

import json
from unittest import mock

from airflow.utils.log.callback_log_reader import (
    _get_callback_log_relative_path,
    _read_callback_local_logs,
    _read_callback_remote_logs,
    read_callback_log,
)


class TestGetCallbackLogRelativePath:
    def test_constructs_correct_path(self):
        path = _get_callback_log_relative_path("my_dag", "run_123", "cb-uuid-456")
        assert path == "executor_callbacks/my_dag/run_123/cb-uuid-456"

    def test_handles_special_characters_in_dag_id(self):
        path = _get_callback_log_relative_path("dag.with.dots", "run-1", "uuid")
        assert path == "executor_callbacks/dag.with.dots/run-1/uuid"


class TestReadCallbackLocalLogs:
    def test_reads_local_log_file(self, tmp_path):
        # Create the expected directory structure
        log_dir = tmp_path / "executor_callbacks" / "test_dag" / "test_run"
        log_dir.mkdir(parents=True)
        log_file = log_dir / "test_callback_id"
        log_content = json.dumps({"timestamp": "2024-01-01T00:00:00", "event": "test log"}) + "\n"
        log_file.write_text(log_content)

        with mock.patch("airflow.utils.log.callback_log_reader.conf") as mock_conf:
            mock_conf.get.return_value = str(tmp_path)
            sources, streams = _read_callback_local_logs(
                "executor_callbacks/test_dag/test_run/test_callback_id"
            )

        assert len(sources) == 1
        assert str(log_file) in sources[0]
        assert len(streams) == 1
        # Consume the stream
        lines = list(streams[0])
        assert len(lines) == 1

    def test_returns_empty_when_no_log_file(self, tmp_path):
        with mock.patch("airflow.utils.log.callback_log_reader.conf") as mock_conf:
            mock_conf.get.return_value = str(tmp_path)
            sources, streams = _read_callback_local_logs("executor_callbacks/nonexistent/run/callback")

        assert sources == []
        assert streams == []


class TestReadCallbackRemoteLogs:
    @mock.patch("airflow.utils.log.callback_log_reader.get_remote_task_log")
    def test_returns_empty_when_no_remote_configured(self, mock_get_remote):
        mock_get_remote.return_value = None
        sources, streams = _read_callback_remote_logs("some/path")
        assert sources == []
        assert streams == []

    @mock.patch("airflow.utils.log.callback_log_reader.get_remote_task_log")
    def test_uses_stream_method_when_available(self, mock_get_remote):
        mock_remote_io = mock.MagicMock()
        mock_remote_io.stream.return_value = (["source1"], [iter(["line1\n", "line2\n"])])
        mock_get_remote.return_value = mock_remote_io

        sources, streams = _read_callback_remote_logs("some/path")

        mock_remote_io.stream.assert_called_once_with("some/path", None)
        assert sources == ["source1"]
        assert len(streams) == 1

    @mock.patch("airflow.utils.log.callback_log_reader.get_remote_task_log")
    def test_falls_back_to_read_method(self, mock_get_remote):
        mock_remote_io = mock.MagicMock(spec=["read"])
        # Remove stream attribute
        del mock_remote_io.stream
        mock_remote_io.read.return_value = (["source1"], ["log content\n"])
        mock_get_remote.return_value = mock_remote_io

        sources, streams = _read_callback_remote_logs("some/path")

        mock_remote_io.read.assert_called_once_with("some/path", None)
        assert sources == ["source1"]
        assert len(streams) == 1

    @mock.patch("airflow.utils.log.callback_log_reader.get_remote_task_log")
    def test_returns_empty_when_read_returns_none(self, mock_get_remote):
        mock_remote_io = mock.MagicMock(spec=["read"])
        del mock_remote_io.stream
        mock_remote_io.read.return_value = ([], None)
        mock_get_remote.return_value = mock_remote_io

        sources, streams = _read_callback_remote_logs("some/path")

        assert sources == []
        assert streams == []


class TestReadCallbackLog:
    @mock.patch("airflow.utils.log.callback_log_reader._read_callback_remote_logs")
    @mock.patch("airflow.utils.log.callback_log_reader._read_callback_local_logs")
    def test_yields_no_logs_message_when_nothing_found(self, mock_local, mock_remote):
        mock_remote.return_value = ([], [])
        mock_local.return_value = ([], [])

        messages = list(read_callback_log("dag1", "run1", "cb1"))
        assert len(messages) == 1
        assert messages[0].event == "No callback logs found."

    @mock.patch("airflow.utils.log.callback_log_reader._read_callback_remote_logs")
    @mock.patch("airflow.utils.log.callback_log_reader._read_callback_local_logs")
    def test_reads_from_remote_when_available(self, mock_local, mock_remote):
        def _log_stream():
            yield '{"timestamp": "2024-01-01T00:00:00", "event": "remote log"}\n'

        mock_remote.return_value = (["s3://bucket/path"], [_log_stream()])
        mock_local.return_value = ([], [])

        messages = list(read_callback_log("dag1", "run1", "cb1"))
        # Should have source header + endgroup + actual log content
        assert len(messages) >= 3
        assert messages[0].event == "::group::Log message source details"
        assert messages[1].event == "::endgroup::"
        # Local should NOT be called when remote succeeds
        mock_local.assert_not_called()

    @mock.patch("airflow.utils.log.callback_log_reader._read_callback_remote_logs")
    @mock.patch("airflow.utils.log.callback_log_reader._read_callback_local_logs")
    def test_falls_back_to_local_when_remote_empty(self, mock_local, mock_remote):
        mock_remote.return_value = ([], [])

        def _log_stream():
            yield '{"timestamp": "2024-01-01T00:00:00", "event": "local log"}\n'

        mock_local.return_value = (["/var/log/airflow/path"], [_log_stream()])

        messages = list(read_callback_log("dag1", "run1", "cb1"))
        assert len(messages) >= 3
        mock_local.assert_called_once()
