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

import datetime
import logging
import socket
import time
from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar, Union, cast

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.metrics.otel import MetricsMap, SafeOtelLogger
from airflow.metrics.validators import AllowListValidator, BlockListValidator, ListValidator, validate_stat
from airflow.typing_compat import Protocol

if TYPE_CHECKING:
    from datadog import DogStatsd
    from statsd import StatsClient

log = logging.getLogger(__name__)
DeltaType = Union[int, float, datetime.timedelta]


class TimerProtocol(Protocol):
    """Type protocol for StatsLogger.timer."""

    def __enter__(self) -> Timer:
        ...

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        ...

    def start(self) -> Timer:
        """Start the timer."""
        ...

    def stop(self, send: bool = True) -> None:
        """Stop, and (by default) submit the timer to StatsD."""
        ...


class StatsLogger(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, etc)."""

    @classmethod
    def incr(
        cls,
        stat: str,
        count: int = 1,
        rate: int | float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Increment stat."""

    @classmethod
    def decr(
        cls,
        stat: str,
        count: int = 1,
        rate: int | float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Decrement stat."""

    @classmethod
    def gauge(
        cls,
        stat: str,
        value: float,
        rate: int | float = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Gauge stat."""

    @classmethod
    def timing(
        cls,
        stat: str,
        dt: DeltaType | None,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Stats timing."""

    @classmethod
    def timer(cls, *args, **kwargs) -> TimerProtocol:
        """Timer metric that can be cancelled."""
        raise NotImplementedError()


class Timer(TimerProtocol):
    """
    Timer that records duration, and optional sends to StatsD backend.

    This class lets us have an accurate timer with the logic in one place (so
    that we don't use datetime math for duration -- it is error prone).

    Example usage:

    .. code-block:: python

        with Stats.timer() as t:
            # Something to time
            frob_the_foos()

        log.info("Frobbing the foos took %.2f", t.duration)

    Or without a context manager:

    .. code-block:: python

        timer = Stats.timer().start()

        # Something to time
        frob_the_foos()

        timer.end()

        log.info("Frobbing the foos took %.2f", timer.duration)

    To send a metric:

    .. code-block:: python

        with Stats.timer("foos.frob"):
            # Something to time
            frob_the_foos()

    Or both:

    .. code-block:: python

        with Stats.timer("foos.frob") as t:
            # Something to time
            frob_the_foos()

        log.info("Frobbing the foos took %.2f", t.duration)
    """

    # pystatsd and dogstatsd both have a timer class, but present different API
    # so we can't use this as a mixin on those, instead this class contains the "real" timer

    _start_time: float | None
    duration: float | None

    def __init__(self, real_timer: Timer | None = None) -> None:
        self.real_timer = real_timer

    def __enter__(self) -> Timer:
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()

    def start(self) -> Timer:
        """Start the timer."""
        if self.real_timer:
            self.real_timer.start()
        self._start_time = time.perf_counter()
        return self

    def stop(self, send: bool = True) -> None:
        """Stop the timer, and optionally send it to stats backend."""
        if self._start_time is not None:
            self.duration = time.perf_counter() - self._start_time
        if send and self.real_timer:
            self.real_timer.stop()


T = TypeVar("T", bound=Callable)


def prepare_stat_with_tags(fn: T) -> T:
    """Add tags to stat with influxdb standard format if influxdb_tags_enabled is True."""

    @wraps(fn)
    def wrapper(
        self, stat: str | None = None, *args, tags: dict[str, str] | None = None, **kwargs
    ) -> Callable[[str], str]:
        if self.influxdb_tags_enabled:
            if stat is not None and tags is not None:
                for k, v in tags.items():
                    if self.metric_tags_validator.test(k):
                        if all((c not in [",", "="] for c in v + k)):
                            stat += f",{k}={v}"
                        else:
                            log.error("Dropping invalid tag: %s=%s.", k, v)
        return fn(self, stat, *args, tags=tags, **kwargs)

    return cast(T, wrapper)


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
                cls.__class__.factory = cls.get_dogstatsd_logger
            elif conf.getboolean("metrics", "statsd_on"):
                cls.__class__.factory = cls.get_statsd_logger
            elif conf.getboolean("metrics", "otel_on"):
                cls.__class__.factory = cls.get_otel_logger
            else:
                cls.__class__.factory = NoStatsLogger

    @classmethod
    def get_statsd_logger(cls) -> SafeStatsdLogger:
        """Returns logger for StatsD."""
        # no need to check for the scheduler/statsd_on -> this method is only called when it is set
        # and previously it would crash with None is callable if it was called without it.
        from statsd import StatsClient

        stats_class = conf.getimport("metrics", "statsd_custom_client_path", fallback=None)
        metrics_validator: ListValidator

        if stats_class:
            if not issubclass(stats_class, StatsClient):
                raise AirflowConfigException(
                    "Your custom StatsD client must extend the statsd.StatsClient in order to ensure "
                    "backwards compatibility."
                )
            else:
                log.info("Successfully loaded custom StatsD client")

        else:
            stats_class = StatsClient

        statsd = stats_class(
            host=conf.get("metrics", "statsd_host"),
            port=conf.getint("metrics", "statsd_port"),
            prefix=conf.get("metrics", "statsd_prefix"),
        )
        if conf.get("metrics", "metrics_allow_list", fallback=None):
            metrics_validator = AllowListValidator(conf.get("metrics", "metrics_allow_list"))
            if conf.get("metrics", "metrics_block_list", fallback=None):
                log.warning(
                    "Ignoring metrics_block_list as both metrics_allow_list "
                    "and metrics_block_list have been set"
                )
        elif conf.get("metrics", "metrics_block_list", fallback=None):
            metrics_validator = BlockListValidator(conf.get("metrics", "metrics_block_list"))
        else:
            metrics_validator = AllowListValidator()
        influxdb_tags_enabled = conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False)
        metric_tags_validator = BlockListValidator(conf.get("metrics", "statsd_disabled_tags", fallback=None))
        return SafeStatsdLogger(statsd, metrics_validator, influxdb_tags_enabled, metric_tags_validator)

    @classmethod
    def get_dogstatsd_logger(cls) -> SafeDogStatsdLogger:
        """Get DataDog StatsD logger."""
        from datadog import DogStatsd

        metrics_validator: ListValidator

        dogstatsd = DogStatsd(
            host=conf.get("metrics", "statsd_host"),
            port=conf.getint("metrics", "statsd_port"),
            namespace=conf.get("metrics", "statsd_prefix"),
            constant_tags=cls.get_constant_tags(),
        )
        if conf.get("metrics", "metrics_allow_list", fallback=None):
            metrics_validator = AllowListValidator(conf.get("metrics", "metrics_allow_list"))
            if conf.get("metrics", "metrics_block_list", fallback=None):
                log.warning(
                    "Ignoring metrics_block_list as both metrics_allow_list "
                    "and metrics_block_list have been set"
                )
        elif conf.get("metrics", "metrics_block_list", fallback=None):
            metrics_validator = BlockListValidator(conf.get("metrics", "metrics_block_list"))
        else:
            metrics_validator = AllowListValidator()
        datadog_metrics_tags = conf.getboolean("metrics", "statsd_datadog_metrics_tags", fallback=True)
        metric_tags_validator = BlockListValidator(conf.get("metrics", "statsd_disabled_tags", fallback=None))
        return SafeDogStatsdLogger(dogstatsd, metrics_validator, datadog_metrics_tags, metric_tags_validator)

    @classmethod
    def get_otel_logger(cls):
        """Get Otel logger"""
        # TODO: wrap this in a try/except with a useful message about missing a required value ??
        host = conf.get("metrics", "otel_host")  # ex: "breeze-otel-collector"
        port = conf.getint("metrics", "otel_port")  # ex: 4318
        prefix = conf.get("metrics", "otel_prefix")  # ex: "airflow"
        interval = conf.getint("metrics", "otel_interval_milliseconds")  # ex: 30000

        allow_list = conf.get("metrics", "metrics_allow_list", fallback=None)
        allow_list_validator = AllowListValidator(allow_list)

        resource = Resource(attributes={SERVICE_NAME: "Airflow"})
        # TODO:  figure out https instead of http ??
        endpoint = f"http://{host}:{port}/v1/metrics"

        print(f"[Metric Exporter] Connecting to OTLP at ---> {endpoint}")
        readers = [
            PeriodicExportingMetricReader(
                OTLPMetricExporter(
                    endpoint=endpoint,
                    headers={"Content-Type": "application/json"},
                ),
                export_interval_millis=interval,
            )
        ]

        # TODO:  remove this console exporter before merge
        debug = False
        if debug:
            export_to_console = PeriodicExportingMetricReader(ConsoleMetricExporter())
            readers.append(export_to_console)

        # TODO: Here and in the return statement:
        #       I like the metrics.foo() here for clarity, but maybe import these directly?
        metrics.set_meter_provider(
            MeterProvider(
                resource=resource,
                metric_readers=readers,
                shutdown_on_exit=False,
            ),
        )

        return SafeOtelLogger(metrics.get_meter_provider(), prefix, allow_list_validator)
        # -----------------------------------------------------------------------------------------

        # -----------------------------------------------------------------------------------------
        # TODO:  the following comment is copypasta from POC2 and not confirmed
        # if we do not set 'shutdown_on_exit' to False, somehow(?) the
        # MeterProvider will constantly get shutdown every second
        # something having to do with the following code:
        # if shutdown_on_exit:
        #     self._atexit_handler = register(self.shutdown)
        # not sure why...
        # metrics.set_meter_provider(MeterProvider(metric_readers=[reader], shutdown_on_exit=False))
        # -----------------------------------------------------------------------------------------

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
