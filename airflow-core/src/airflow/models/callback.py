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
from enum import Enum
from importlib import import_module
from typing import TYPE_CHECKING

import uuid6
from sqlalchemy import Column, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy_utils import UUIDType

from airflow._shared.timezones import timezone
from airflow.models import Base
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.triggers.base import TriggerEvent

log = logging.getLogger(__name__)


class CallbackState(str, Enum):
    """All possible states of callbacks."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


ACTIVE_STATES = frozenset((CallbackState.QUEUED, CallbackState.RUNNING))
TERMINAL_STATES = frozenset((CallbackState.SUCCESS, CallbackState.FAILED))


class CallbackType(str, Enum):
    """
    Types of Callbacks.

    Used:
        - for figuring out what class to instantiate while deserialization
        - by the executor (once implemented) to figure out how to interpret `Callback.data`
    """

    TRIGGERER = "triggerer"
    EXECUTOR = "executor"
    DAG_PROCESSOR = "dag_processor"


class Callback(Base):
    """A generic callback."""

    __tablename__ = "callback"

    id = Column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)

    # This is used by SQLAlchemy to be able to deserialize DB rows to subclasses
    __mapper_args__ = {
        "polymorphic_identity": "callback",
        "polymorphic_on": "callback_type",
    }
    callback_type = Column(String(20), nullable=False)

    # Used by subclasses to store information about how to run the callback
    data = Column(ExtendedJSON)

    # State of the Callback of type: CallbackType
    state = Column(String(10))

    # Return value of the callback if successful, otherwise exception details
    output = Column(Text)

    # Used for prioritization which is not currently implemented. Higher weight = higher priority
    priority_weight = Column(Integer, nullable=False)

    # Creation time of the callback
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)

    # Used for callbacks of type CallbackType.TRIGGERER
    trigger_id = Column(Integer, ForeignKey("trigger.id"), nullable=True)
    trigger = relationship("Trigger", back_populates="callback", uselist=False)

    def __init__(self, priority_weight: int = 10, state: str | None = None, data: dict | None = None):
        self.created_at = timezone.utcnow()
        self.state = state if state else CallbackState.PENDING
        self.priority_weight = priority_weight
        self.data = data

    def queue(self):
        self.state = CallbackState.QUEUED


class TriggererCallback(Callback):
    """Callbacks that run on the Triggerer (must be async)."""

    __mapper_args__ = {"polymorphic_identity": CallbackType.TRIGGERER}

    def __init__(self, callback_def):
        super().__init__(data=callback_def.serialize())

    def queue(self):
        from airflow.models.trigger import Trigger
        from airflow.triggers.callback import CallbackTrigger

        self.trigger = Trigger.from_object(
            CallbackTrigger(
                callback_path=self.data["path"],
                callback_kwargs=self.data["kwargs"],
            )
        )
        super().queue()

    def handle_event(self, event: TriggerEvent, session: Session):
        from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY

        if (status := event.payload.get(PAYLOAD_STATUS_KEY)) and status in (ACTIVE_STATES | TERMINAL_STATES):
            self.state = status
            if status in TERMINAL_STATES:
                self.trigger = None
                self.output = event.payload.get(PAYLOAD_BODY_KEY)
            session.add(self)
        else:
            log.error("Unexpected event received: %s", event.payload)


class ExecutorCallback(Callback):
    """
    Callbacks that run on the executor.

    Not currently supported.
    """

    __mapper_args__ = {"polymorphic_identity": CallbackType.EXECUTOR}

    def __init__(self, callback_def):
        super().__init__(data=callback_def.serialize())


class DagProcessorCallback(Callback):
    """Used to store Dag Processor callbacks in the DB."""

    __mapper_args__ = {"polymorphic_identity": CallbackType.DAG_PROCESSOR}

    def __init__(self, priority_weight: int, callback: CallbackRequest):
        super().__init__(
            priority_weight=priority_weight,
            data={"request_type": callback.__class__.__name__, "request_data": callback.to_json()},
        )
        self.state = None

    def get_callback_request(self) -> CallbackRequest:
        module = import_module("airflow.callbacks.callback_requests")
        callback_request_class = getattr(module, self.data["request_type"])
        # Get the function (from the instance) that we need to call
        from_json = getattr(callback_request_class, "from_json")
        return from_json(self.data["request_data"])


def create_callback_from_def(callback_def) -> Callback:
    match type(callback_def).__name__:
        case "AsyncCallback":
            return TriggererCallback(callback_def)
        case "SyncCallback":
            return ExecutorCallback(callback_def)
        case _:
            raise RuntimeError(f"Cannot handle Callback of type {type(callback_def)}")
