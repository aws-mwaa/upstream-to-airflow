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
"""Tests for the callback supervisor module."""

from __future__ import annotations

import signal
import socket
from dataclasses import dataclass
from operator import attrgetter
from typing import Any
from unittest.mock import patch

import pytest
import structlog

from airflow.sdk.execution_time.callback_supervisor import CallbackSubprocess, execute_callback
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    GetConnection,
    GetVariable,
    MaskSecret,
    VariableResult,
    _RequestFrame,
)


def callback_no_args():
    """A simple callback that takes no arguments."""
    return "ok"


def callback_with_kwargs(arg1, arg2):
    """A callback that accepts keyword arguments."""
    return f"{arg1}-{arg2}"


def callback_that_raises():
    """A callback that always raises."""
    raise ValueError("something went wrong")


class CallableClass:
    """A class that returns a callable instance (like BaseNotifier)."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, context):
        return "notified"


class TestExecuteCallback:
    @pytest.mark.parametrize(
        ("path", "kwargs", "expect_success", "error_contains"),
        [
            pytest.param(
                f"{__name__}.callback_no_args",
                {},
                True,
                None,
                id="successful_no_args",
            ),
            pytest.param(
                f"{__name__}.callback_with_kwargs",
                {"arg1": "hello", "arg2": "world"},
                True,
                None,
                id="successful_with_kwargs",
            ),
            pytest.param(
                f"{__name__}.CallableClass",
                {"msg": "alert"},
                True,
                None,
                id="callable_class_pattern",
            ),
            pytest.param(
                "",
                {},
                False,
                "Callback path not found",
                id="empty_path",
            ),
            pytest.param(
                "nonexistent.module.function",
                {},
                False,
                "ModuleNotFoundError",
                id="import_error",
            ),
            pytest.param(
                f"{__name__}.callback_that_raises",
                {},
                False,
                "ValueError",
                id="execution_error",
            ),
            pytest.param(
                f"{__name__}.nonexistent_function_xyz",
                {},
                False,
                "AttributeError",
                id="attribute_error",
            ),
        ],
    )
    def test_execute_callback(self, path, kwargs, expect_success, error_contains):
        log = structlog.get_logger()
        success, error = execute_callback(path, kwargs, log)

        assert success is expect_success
        if error_contains:
            assert error_contains in error
        else:
            assert error is None


class TestCallbackHandleRequest:
    """Verify that CallbackSubprocess._handle_request dispatches each message type to the correct handler."""

    @dataclass
    class ClientMock:
        method_path: str
        args: tuple = ()
        kwargs: dict | None = None
        response: Any = None

        def __post_init__(self):
            if self.kwargs is None:
                self.kwargs = {}

    @dataclass
    class RequestCase:
        message: Any
        test_id: str
        client_mock: Any = None  # Should be ClientMock but Python can't forward-ref sibling nested classes
        mask_secret_args: tuple | None = None

    REQUEST_CASES = [
        RequestCase(
            message=GetConnection(conn_id="test_conn"),
            test_id="get_connection",
            client_mock=ClientMock(
                method_path="connections.get",
                args=("test_conn",),
                response=ConnectionResult(conn_id="test_conn", conn_type="mysql"),
            ),
        ),
        RequestCase(
            message=GetConnection(conn_id="test_conn"),
            test_id="get_connection_with_password",
            client_mock=ClientMock(
                method_path="connections.get",
                args=("test_conn",),
                response=ConnectionResult(conn_id="test_conn", conn_type="mysql", password="secret"),
            ),
            mask_secret_args=("secret",),
        ),
        RequestCase(
            message=GetVariable(key="test_key"),
            test_id="get_variable",
            client_mock=ClientMock(
                method_path="variables.get",
                args=("test_key",),
                response=VariableResult(key="test_key", value="test_value"),
            ),
        ),
        RequestCase(
            message=MaskSecret(value="super_secret", name="api_key"),
            test_id="mask_secret",
            mask_secret_args=("super_secret", "api_key"),
        ),
    ]

    @pytest.fixture
    def callback_subprocess(self, mocker):
        read_end, write_end = socket.socketpair()
        proc = CallbackSubprocess(
            process_log=mocker.MagicMock(),
            id="12345678-1234-5678-1234-567812345678",
            pid=12345,
            stdin=write_end,
            client=mocker.Mock(),
            process=mocker.Mock(),
        )
        return proc, read_end

    @patch("airflow.sdk.execution_time.request_handlers.mask_secret")
    @pytest.mark.parametrize("test_case", REQUEST_CASES, ids=lambda tc: tc.test_id)
    def test_handle_requests(
        self,
        mock_mask_secret,
        callback_subprocess,
        mocker,
        test_case,
    ):
        client_mock = test_case.client_mock

        proc, _read_end = callback_subprocess

        if client_mock:
            mock_client_method = attrgetter(client_mock.method_path)(proc.client)
            mock_client_method.return_value = client_mock.response

        generator = proc.handle_requests(log=mocker.Mock())
        next(generator)

        req_frame = _RequestFrame(id=42, body=test_case.message.model_dump())
        generator.send(req_frame)

        if test_case.mask_secret_args is not None:
            mock_mask_secret.assert_called_with(*test_case.mask_secret_args)

        if client_mock:
            mock_client_method.assert_called_once_with(*client_mock.args, **client_mock.kwargs)


class TestCallbackExecutionTimeout:
    """Verify that CallbackSubprocess kills the subprocess when the execution timeout is exceeded."""

    @pytest.fixture
    def callback_subprocess(self, mocker):
        read_end, write_end = socket.socketpair()
        proc = CallbackSubprocess(
            process_log=mocker.MagicMock(),
            id="12345678-1234-5678-1234-567812345678",
            pid=12345,
            stdin=write_end,
            client=mocker.Mock(),
            process=mocker.Mock(),
        )
        return proc, read_end

    def test_timeout_kills_subprocess(self, monkeypatch, mocker, callback_subprocess, captured_logs):
        """When the callback exceeds the configured timeout, the subprocess is terminated."""
        timeout_seconds = 10
        monkeypatch.setattr(
            "airflow.sdk.execution_time.callback_supervisor.CALLBACK_EXECUTION_TIMEOUT", timeout_seconds
        )

        proc, _read_end = callback_subprocess

        # Patch the kill method so we can assert it was called
        mock_kill = mocker.patch("airflow.sdk.execution_time.callback_supervisor.CallbackSubprocess.kill")

        # Simulate time progressing past the timeout.
        # First call returns 0 (start), subsequent calls return past-timeout values.
        call_count = 0

        def mock_monotonic():
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return 0.0
            # After the first call, time has exceeded the timeout
            return timeout_seconds + 1.0

        # Patch _service_subprocess to simulate one loop iteration where the process is still alive
        service_call_count = 0

        def mock_service_subprocess(max_wait_time):
            nonlocal service_call_count
            service_call_count += 1
            # After the kill is called (which sets _exit_code via the mock), stop the loop
            if service_call_count > 1:
                proc._exit_code = -signal.SIGTERM
            return None

        mocker.patch.object(proc, "_service_subprocess", side_effect=mock_service_subprocess)

        with patch(
            "airflow.sdk.execution_time.callback_supervisor.time.monotonic", side_effect=mock_monotonic
        ):
            proc._monitor_subprocess()

        mock_kill.assert_called_once_with(signal.SIGTERM, force=True)
        assert any(
            "Callback execution timeout reached" in record.get("event", "") for record in captured_logs
        )

    def test_no_timeout_when_disabled(self, monkeypatch, mocker, callback_subprocess):
        """When timeout is 0, the subprocess is never killed for exceeding a time limit."""
        monkeypatch.setattr("airflow.sdk.execution_time.callback_supervisor.CALLBACK_EXECUTION_TIMEOUT", 0)

        proc, _read_end = callback_subprocess

        mock_kill = mocker.patch("airflow.sdk.execution_time.callback_supervisor.CallbackSubprocess.kill")

        # Simulate time progressing well past any reasonable timeout
        call_count = 0

        def mock_monotonic():
            nonlocal call_count
            call_count += 1
            # Return ever-increasing time (simulating long-running callback)
            return call_count * 1000.0

        # Subprocess exits normally after one iteration
        service_call_count = 0

        def mock_service_subprocess(max_wait_time):
            nonlocal service_call_count
            service_call_count += 1
            if service_call_count >= 2:
                proc._exit_code = 0
            return None

        mocker.patch.object(proc, "_service_subprocess", side_effect=mock_service_subprocess)

        with patch(
            "airflow.sdk.execution_time.callback_supervisor.time.monotonic", side_effect=mock_monotonic
        ):
            proc._monitor_subprocess()

        # kill should never be called when timeout is disabled
        mock_kill.assert_not_called()
