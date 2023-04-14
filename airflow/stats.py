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
from __future__ import annotations

import logging
import socket
from typing import TYPE_CHECKING, Callable
import string
import time
import warnings
from functools import partial, wraps
from typing import TYPE_CHECKING, Callable, Iterable, TypeVar, Union, cast

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from airflow.configuration import conf
from airflow.metrics import datadog_logger, statsd_logger
from airflow.metrics.protocols import DeltaType, StatsLogger, Timer, TimerProtocol
from airflow.exceptions import AirflowConfigException, InvalidStatsNameException
from airflow.stats_clients.otel import CounterMap
from airflow.typing_compat import Protocol

if TYPE_CHECKING:
    from datadog import DogStatsd
    from statsd import StatsClient

log = logging.getLogger(__name__)


class NoStatsLogger:
    """If no StatsLogger is configured, NoStatsLogger is used as a fallback."""

    @classmethod
    def incr(cls, stat: str, count: int = 1, rate: int = 1, *, tags: dict[str, str] | None = None) -> None:
        """Increment stat."""

    @classmethod
    def decr(cls, stat: str, count: int = 1, rate: int = 1, *, tags: dict[str, str] | None = None) -> None:
        """Decrement stat."""

    @classmethod
    def gauge(
        cls,
        stat: str,
        value: int,
        rate: int = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Gauge stat."""

    @classmethod
    def timing(cls, stat: str, dt: DeltaType, *, tags: dict[str, str] | None = None) -> None:
        """Stats timing."""

    @classmethod
    def timer(cls, *args, **kwargs) -> TimerProtocol:
        """Timer metric that can be cancelled."""
        return Timer()


class SafeStatsdLogger:
    """StatsD Logger."""

    def __init__(
        self,
        statsd_client: StatsClient,
        metrics_validator: ListValidator = AllowListValidator(),
        influxdb_tags_enabled: bool = False,
        metric_tags_validator: ListValidator = AllowListValidator(),
    ) -> None:
        self.statsd = statsd_client
        self.metrics_validator = metrics_validator
        self.influxdb_tags_enabled = influxdb_tags_enabled
        self.metric_tags_validator = metric_tags_validator

    @prepare_stat_with_tags
    @validate_stat
    def incr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Increment stat."""
        if self.metrics_validator.test(stat):
            return self.statsd.incr(stat, count, rate)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def decr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Decrement stat."""
        if self.metrics_validator.test(stat):
            return self.statsd.decr(stat, count, rate)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def gauge(
        self,
        stat: str,
        value: int | float,
        rate: float = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Gauge stat."""
        if self.metrics_validator.test(stat):
            return self.statsd.gauge(stat, value, rate, delta)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def timing(
        self,
        stat: str,
        dt: DeltaType,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Stats timing."""
        if self.metrics_validator.test(stat):
            return self.statsd.timing(stat, dt)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def timer(
        self,
        stat: str | None = None,
        *args,
        tags: dict[str, str] | None = None,
        **kwargs,
    ) -> TimerProtocol:
        """Timer metric that can be cancelled."""
        if stat and self.metrics_validator.test(stat):
            return Timer(self.statsd.timer(stat, *args, **kwargs))
        return Timer()


class SafeDogStatsdLogger:
    """DogStatsd Logger."""

    def __init__(
        self,
        dogstatsd_client: DogStatsd,
        metrics_validator: ListValidator = AllowListValidator(),
        metrics_tags: bool = False,
        metric_tags_validator: ListValidator = AllowListValidator(),
    ) -> None:
        self.dogstatsd = dogstatsd_client
        self.metrics_validator = metrics_validator
        self.metrics_tags = metrics_tags
        self.metric_tags_validator = metric_tags_validator

    @validate_stat
    def incr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Increment stat."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [
                f"{key}:{value}" for key, value in tags.items() if self.metric_tags_validator.test(key)
            ]
        else:
            tags_list = []
        if self.metrics_validator.test(stat):
            return self.dogstatsd.increment(metric=stat, value=count, tags=tags_list, sample_rate=rate)
        return None

    @validate_stat
    def decr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Decrement stat."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [
                f"{key}:{value}" for key, value in tags.items() if self.metric_tags_validator.test(key)
            ]
        else:
            tags_list = []
        if self.metrics_validator.test(stat):
            return self.dogstatsd.decrement(metric=stat, value=count, tags=tags_list, sample_rate=rate)
        return None

    @validate_stat
    def gauge(
        self,
        stat: str,
        value: int | float,
        rate: float = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Gauge stat."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [
                f"{key}:{value}" for key, value in tags.items() if self.metric_tags_validator.test(key)
            ]
        else:
            tags_list = []
        if self.metrics_validator.test(stat):
            return self.dogstatsd.gauge(metric=stat, value=value, tags=tags_list, sample_rate=rate)
        return None

    @validate_stat
    def timing(
        self,
        stat: str,
        dt: DeltaType,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Stats timing."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [
                f"{key}:{value}" for key, value in tags.items() if self.metric_tags_validator.test(key)
            ]
        else:
            tags_list = []
        if self.metrics_validator.test(stat):
            if isinstance(dt, datetime.timedelta):
                dt = dt.total_seconds()
            return self.dogstatsd.timing(metric=stat, value=dt, tags=tags_list)
        return None

    @validate_stat
    def timer(
        self,
        stat: str | None = None,
        tags: dict[str, str] | None = None,
        **kwargs,
    ) -> TimerProtocol:
        """Timer metric that can be cancelled."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [
                f"{key}:{value}" for key, value in tags.items() if self.metric_tags_validator.test(key)
            ]
        else:
            tags_list = []
        if stat and self.metrics_validator.test(stat):
            return Timer(self.dogstatsd.timed(stat, tags=tags_list, **kwargs))
        return Timer()


class SafeOtelLogger:
    """Otel Logger"""

    def __init__(self, otel_provider, prefix: str = "airflow", allow_list_validator=AllowListValidator()):
        self.otel: Callable = otel_provider
        self.prefix: str = prefix
        self.metrics_validator = allow_list_validator
        self.meter = otel_provider.get_meter(__name__)
        self.counter_map = CounterMap(self.meter)

    @validate_stat
    def incr(self, stat: str, count: int = 1, rate: float = 1, tags: dict[str, str] | None = None):
        """
        Increment stat by count.

        :param stat: The name of the stat to increment.
        :param count: A positive integer to add to the current value of stat.
        :param rate: TODO: define me
        :param tags: Tags to append to the stat.
        """
        if (count < 0) or (rate < 0):
            raise ValueError("count and rate must both be positive integers.")
        # TODO: I don't think this is the right use for rate???
        value = count * rate

        if self.metrics_validator.test(stat):
            counter = self.counter_map.get_counter(f"{self.prefix}.{stat}")
            return counter.add(value, attributes=tags)

    @validate_stat
    def decr(self, stat: str, count: int = 1, rate: float = 1, tags: dict[str, str] | None = None):
        """
        Decrement stat by count.

        :param stat: The name of the stat to increment.
        :param count: A positive integer to subtract from current value of stat.
        :param rate: TODO: define me
        :param tags: Tags to append to the stat.
        """
        if (count < 0) or (rate < 0):
            raise ValueError("count and rate must both be positive values.")
        # TODO: I don't think this is the right use for rate???
        # TODO: abs() isn't needed since we check for negative above, but keep it for clarity or no?
        value = abs(count * rate)

        if self.metrics_validator.test(stat):
            counter = self.counter_map.get_counter(f"{self.prefix}.{stat}")
            return counter.add(-value, attributes=tags)

    @validate_stat
    def gauge(
        self,
        stat: str,
        value: int | float,
        rate: float = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Gauge stat."""
        # To be implemented
        return None

    @validate_stat
    def timing(
        self,
        stat: str,
        dt: DeltaType,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Stats timing."""
        # To be implemented
        return None

    @validate_stat
    def timer(
        self,
        stat: str | None = None,
        *args,
        tags: dict[str, str] | None = None,
        **kwargs,
    ) -> TimerProtocol:
        """Timer metric that can be cancelled."""
        # To be implemented
        return Timer()


class _Stats(type):
    factory: Callable
    instance: StatsLogger | NoStatsLogger | None = None

    def __getattr__(cls, name: str) -> str:
        if not cls.instance:
            try:
                cls.instance = cls.factory()
            except (socket.gaierror, ImportError) as e:
                log.error("Could not configure StatsClient: %s, using NoStatsLogger instead.", e)
                cls.instance = NoStatsLogger()
        return getattr(cls.instance, name)

    def __init__(cls, *args, **kwargs) -> None:
        super().__init__(cls)
        if not hasattr(cls.__class__, "factory"):
            is_datadog_enabled_defined = conf.has_option("metrics", "statsd_datadog_enabled")
            if is_datadog_enabled_defined and conf.getboolean("metrics", "statsd_datadog_enabled"):
                cls.__class__.factory = datadog_logger.get_dogstatsd_logger
            elif conf.getboolean("metrics", "statsd_on"):
                cls.__class__.factory = statsd_logger.get_statsd_logger
            elif conf.getboolean("metrics", "otel_on"):
                cls.__class__.factory = otel_logger.get_otel_logger
            else:
                cls.__class__.factory = NoStatsLogger

    @classmethod
    def get_constant_tags(cls) -> list[str]:
        """Get constant DataDog tags to add to all stats."""
        tags: list[str] = []
        tags_in_string = conf.get("metrics", "statsd_datadog_tags", fallback=None)
        if tags_in_string is None or tags_in_string == "":
            return tags
        else:
            for key_value in tags_in_string.split(","):
                tags.append(key_value)
            return tags


if TYPE_CHECKING:
    Stats: StatsLogger
else:

    class Stats(metaclass=_Stats):
        """Empty class for Stats - we use metaclass to inject the right one."""
