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

import copy
from unittest import mock

import pytest

from airflow.models.callback import CallbackState
from airflow.sdk import BaseNotifier
from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY, CallbackTrigger

TEST_MESSAGE = "test_message"
TEST_CALLBACK_PATH = "classpath.test_callback"
TEST_CALLBACK_KWARGS = {"message": TEST_MESSAGE, "context": {"dag_run": "test"}}


class ExampleAsyncNotifier(BaseNotifier):
    """Example of a properly implemented async notifier."""

    template_fields = ("message",)

    def __init__(self, message, **kwargs):
        super().__init__(**kwargs)
        self.message = message

    async def async_notify(self, context):
        return f"Async notification: {self.message}, context: {context}"

    def notify(self, context):
        return f"Sync notification: {self.message}, context: {context}"


class TestCallbackTrigger:
    @pytest.fixture
    def trigger(self):
        """Create a fresh trigger per test to avoid shared mutable state.

        Deep copy: BaseNotifier._update_context mutates the nested context dict in
        place, so a shallow copy would leak notifier-test state into other tests.
        """
        return CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs=copy.deepcopy(TEST_CALLBACK_KWARGS),
        )

    @pytest.fixture
    def mock_import_string(self):
        with mock.patch("airflow.triggers.callback.import_string") as m:
            yield m

    @pytest.mark.parametrize(
        ("callback_init_kwargs", "expected_serialized_kwargs"),
        [
            pytest.param(None, {}, id="no kwargs"),
            pytest.param(TEST_CALLBACK_KWARGS, TEST_CALLBACK_KWARGS, id="non-empty kwargs"),
        ],
    )
    def test_serialization(self, callback_init_kwargs, expected_serialized_kwargs):
        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs=callback_init_kwargs,
        )
        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.triggers.callback.CallbackTrigger"
        assert kwargs == {
            "callback_path": TEST_CALLBACK_PATH,
            "callback_kwargs": expected_serialized_kwargs,
        }

    def test_queue_not_inherited_from_task(self):
        """False so `_defer_task` doesn't overwrite the trigger's own `queue` with the task's."""
        assert CallbackTrigger.trigger_queue_inherited_from_task is False

    def test_queue_attribute_is_not_part_of_serialized_kwargs(self):
        """``queue`` is read directly off the trigger by ``Trigger.from_object``, not via serialize()."""
        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs=None,
            queue="custom-queue",
        )

        assert trigger.queue == "custom-queue"
        _, kwargs = trigger.serialize()
        assert "queue" not in kwargs

    def test_queue_defaults_to_none(self, trigger):
        assert trigger.queue is None

    @pytest.mark.asyncio
    async def test_run_success_with_async_function(self, trigger, mock_import_string):
        """Test trigger handles async functions correctly."""
        callback_return_value = "some value"
        mock_callback = mock.AsyncMock(return_value=callback_return_value)
        mock_import_string.return_value = mock_callback

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        success_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        # AsyncMock accepts **kwargs, so accepts_context returns True and context is passed through
        mock_callback.assert_called_once_with(message=TEST_MESSAGE, context={"dag_run": "test"})
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert success_event.payload[PAYLOAD_BODY_KEY] == callback_return_value

    @pytest.mark.asyncio
    async def test_run_success_with_notifier(self, trigger, mock_import_string):
        """Test trigger handles async notifier classes correctly."""
        mock_import_string.return_value = ExampleAsyncNotifier

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        success_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        # BaseNotifier._update_context merges template_fields ("message") into the context.
        assert (
            success_event.payload[PAYLOAD_BODY_KEY]
            == f"Async notification: {TEST_MESSAGE}, context: {{'dag_run': 'test', 'message': '{TEST_MESSAGE}'}}"
        )

    @pytest.mark.asyncio
    async def test_run_failure(self, trigger, mock_import_string):
        exc_msg = "Something went wrong"
        mock_callback = mock.AsyncMock(side_effect=RuntimeError(exc_msg))
        mock_import_string.return_value = mock_callback

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        failure_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        # AsyncMock accepts **kwargs, so accepts_context returns True and context is passed through
        mock_callback.assert_called_once_with(message=TEST_MESSAGE, context={"dag_run": "test"})
        assert failure_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.FAILED
        assert all(s in failure_event.payload[PAYLOAD_BODY_KEY] for s in ["raise", "RuntimeError", exc_msg])


class TestCallbackTriggerContextAndRendering:
    """Runtime context injection and Jinja rendering of callback kwargs."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("callback_kwargs", "context", "expected_call_kwargs"),
        [
            pytest.param(
                {"message": "DAG {{ dag_id }} missed deadline at {{ ds }}"},
                {"dag_id": "my_dag", "ds": "2024-06-15"},
                {"message": "DAG my_dag missed deadline at 2024-06-15"},
                id="renders_templated_kwarg",
            ),
            pytest.param(
                {"message": "plain", "count": 5},
                {"dag_id": "my_dag"},
                {"message": "plain", "count": 5},
                id="untemplated_kwargs_untouched",
            ),
        ],
    )
    async def test_run_renders_kwargs_with_runtime_context(
        self, callback_kwargs, context, expected_call_kwargs
    ):
        trigger = CallbackTrigger(callback_path=TEST_CALLBACK_PATH, callback_kwargs=callback_kwargs)
        trigger._callback_context = context

        mock_callback = mock.AsyncMock(return_value="ok")
        with mock.patch("airflow.triggers.callback.import_string", return_value=mock_callback):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        mock_callback.assert_called_once_with(**expected_call_kwargs, context=context)

    @pytest.mark.asyncio
    async def test_run_prefers_runtime_context_over_stored_context(self):
        """A 3.2.x-serialized context in kwargs is dropped when a runtime context is set."""
        stored_context = {"dag_id": "stale"}
        runtime_context = {"dag_id": "fresh"}
        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs={"message": "{{ dag_id }}", "context": stored_context},
        )
        trigger._callback_context = runtime_context

        mock_callback = mock.AsyncMock(return_value="ok")
        with mock.patch("airflow.triggers.callback.import_string", return_value=mock_callback):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        mock_callback.assert_called_once_with(message="fresh", context=runtime_context)

    @pytest.mark.asyncio
    async def test_run_falls_back_to_stored_context(self):
        """Without a runtime context, the 3.2.x context stored in kwargs is used."""
        stored_context = {"dag_id": "stored"}
        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs={"message": "{{ dag_id }}", "context": stored_context},
        )

        mock_callback = mock.AsyncMock(return_value="ok")
        with mock.patch("airflow.triggers.callback.import_string", return_value=mock_callback):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        mock_callback.assert_called_once_with(message="stored", context=stored_context)

    @pytest.mark.asyncio
    async def test_run_skips_rendering_without_context(self):
        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs={"message": "{{ dag_id }}"},
        )

        mock_callback = mock.AsyncMock(return_value="ok")
        with mock.patch("airflow.triggers.callback.import_string", return_value=mock_callback):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        mock_callback.assert_called_once_with(message="{{ dag_id }}")

    @pytest.mark.asyncio
    async def test_notifier_receives_context_and_renders_template_fields(self):
        """Notifier template_fields render via __await__ using the runtime context."""
        context = {"dag_id": "my_dag", "ds": "2024-06-15"}
        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs={"message": "Alert for {{ dag_id }} on {{ ds }}"},
        )
        trigger._callback_context = context

        with mock.patch("airflow.triggers.callback.import_string", return_value=ExampleAsyncNotifier):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert "Alert for my_dag on 2024-06-15" in events[-1].payload[PAYLOAD_BODY_KEY]

    @pytest.mark.asyncio
    async def test_run_does_not_mutate_stored_kwargs(self):
        """Rendering must not mutate callback_kwargs — the trigger may be re-serialized."""
        callback_kwargs = {"message": "{{ dag_id }}", "context": {"dag_id": "stored"}}
        trigger = CallbackTrigger(callback_path=TEST_CALLBACK_PATH, callback_kwargs=callback_kwargs)

        mock_callback = mock.AsyncMock(return_value="ok")
        with mock.patch("airflow.triggers.callback.import_string", return_value=mock_callback):
            [event async for event in trigger.run()]

        assert trigger.callback_kwargs == {"message": "{{ dag_id }}", "context": {"dag_id": "stored"}}
