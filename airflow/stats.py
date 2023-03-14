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

import abc
import datetime
import logging
import socket
import string
import time
import warnings
from functools import partial, wraps
from typing import TYPE_CHECKING, Callable, Iterable, TypeVar, Union, cast

from datadog import DogStatsd
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Instrument
from opentelemetry.sdk import util
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.measurement import Measurement
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from statsd import StatsClient

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, InvalidStatsNameException
from airflow.typing_compat import Protocol

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
    # so we can't use this as a mixin on those, instead this class is contains the "real" timer

    _start_time: int | None
    duration: int | None

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
        self._start_time = int(time.perf_counter())
        return self

    def stop(self, send: bool = True) -> None:
        """Stop the timer, and optionally send it to stats backend."""
        self.duration = int(time.perf_counter()) - (self._start_time or 0)
        if send and self.real_timer:
            self.real_timer.stop()


# Only characters in the character set are considered valid
# for the stat_name if stat_name_default_handler is used.
ALLOWED_CHARACTERS = frozenset(string.ascii_letters + string.digits + "_.-")


def stat_name_default_handler(
    stat_name: str, max_length: int = 250, allowed_chars: Iterable[str] = ALLOWED_CHARACTERS
) -> str:
    """
    Validate the StatsD stat name.

    Apply changes when necessary and return the transformed stat name.
    """
    if not isinstance(stat_name, str):
        raise InvalidStatsNameException("The stat_name has to be a string")
    if len(stat_name) > max_length:
        raise InvalidStatsNameException(
            f"The stat_name ({stat_name}) has to be less than {max_length} characters."
        )
    if not all((c in allowed_chars) for c in stat_name):
        raise InvalidStatsNameException(
            f"The stat name ({stat_name}) has to be composed of ASCII "
            f"alphabets, numbers, or the underscore, dot, or dash characters."
        )
    return stat_name


def get_current_handler_stat_name_func() -> Callable[[str], str]:
    """Get Stat Name Handler from airflow.cfg."""
    handler = conf.getimport("metrics", "stat_name_handler")
    if handler is None:
        if conf.get("metrics", "statsd_influxdb_enabled", fallback=False):
            handler = partial(stat_name_default_handler, allowed_chars={*ALLOWED_CHARACTERS, ",", "="})
        else:
            handler = stat_name_default_handler
    return handler


T = TypeVar("T", bound=Callable)


def validate_stat(fn: T) -> T:
    """
    Check if stat name contains invalid characters.
    Log and not emit stats if name is invalid.
    """

    @wraps(fn)
    def wrapper(self, stat: str | None = None, *args, **kwargs) -> T | None:
        try:
            if stat is not None:
                handler_stat_name_func = get_current_handler_stat_name_func()
                stat = handler_stat_name_func(stat)
            return fn(self, stat, *args, **kwargs)
        except InvalidStatsNameException:
            log.exception("Invalid stat name: %s.", stat)
            return None

    return cast(T, wrapper)


class ListValidator(metaclass=abc.ABCMeta):
    """
    ListValidator metaclass that can be implemented as a AllowListValidator
    or BlockListValidator. The test method must be overridden by its subclass.
    """

    def __init__(self, validate_list: str | None = None) -> None:
        self.validate_list: tuple[str, ...] | None = (
            tuple(item.strip().lower() for item in validate_list.split(",")) if validate_list else None
        )

    @classmethod
    def __subclasshook__(cls, subclass: Callable[[str], str]) -> bool:
        return hasattr(subclass, "test") and callable(subclass.test) or NotImplemented

    @abc.abstractmethod
    def test(self, name: str) -> bool:
        """Test if name is allowed"""
        raise NotImplementedError


class AllowListValidator(ListValidator):
    """AllowListValidator only allows names that match the allowed prefixes."""

    def test(self, name: str) -> bool:
        if self.validate_list is not None:
            return name.strip().lower().startswith(self.validate_list)
        else:
            return True  # default is all metrics are allowed


class BlockListValidator(ListValidator):
    """BlockListValidator only allows names that do not match the blocked prefixes."""

    def test(self, name: str) -> bool:
        if self.validate_list is not None:
            return not name.strip().lower().startswith(self.validate_list)
        else:
            return True  # default is all metrics are allowed


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


class SafeOtelLogger:
    """Otel Logger"""

    def __init__(self, otel_provider, prefix: str = "airflow", allow_list_validator=AllowListValidator()):
        self.otel: Callable = otel_provider
        self.prefix: str = prefix
        self.allow_list_validator = allow_list_validator
        self.meter = otel_provider.get_meter(__name__)
        self.counter_map = CounterMap(self.meter)
        self.gauge_map = GaugeMap(self.meter)
        self.meter.create_observable_gauge(name="airflow", callbacks=[self.gauge_map.get_readings])

    @validate_stat
    def incr(self, stat: str, count: int = 1, rate: float = 1, tags: dict[str, str] | None = None):
        """Increment stat"""
        value = count * rate
        # warnings.warn(f"*** INCREMENTING {stat}:\t{value}")

        if self.allow_list_validator.test(stat):
            counter = self.counter_map.get_counter(f"{self.prefix}.{stat}")
            return counter.add(value, attributes=tags)

    @validate_stat
    def decr(self, stat: str, count: int = 1, rate: float = 1, tags: dict[str, str] | None = None):
        """Decrement stat"""
        value = -1 * (count * rate)
        # warnings.warn(f"*** DECREMENTING {stat}:\t{value}")

        if self.allow_list_validator.test(stat):
            counter = self.counter_map.get_counter(f"{self.prefix}.{stat}")
            return counter.add(value, attributes=tags)

    @validate_stat
    def gauge(
        self,
        stat: str,
        value: int,
        tags: dict[str, str] | None = None,
    ):
        """Gauge stat"""
        # warnings.warn(f"****** UPDATING GAUGE ******\n{stat}:\t{value}")
        if self.allow_list_validator.test(stat):
            self.gauge_map.set_value(f"{self.prefix}.{stat}", value, attributes=tags)

    @validate_stat
    def timing(self, stat: str, dt: DeltaType, tags: dict[str, str] | None = None):
        """Stats timing"""
        # warnings.warn(f"*** UPDATE TIMER {stat}:\t{dt}")
        if self.allow_list_validator.test(stat):
            if isinstance(dt, datetime.timedelta):
                dt = dt.total_seconds()
            self.gauge_map.set_value(f"{self.prefix}.{stat}", value=dt, attributes=tags)

    @validate_stat
    def timer(self, stat: str | None = None, attributes: dict[str, str] | None = None, *args, **kwargs):
        """Timer metric that can be cancelled"""
        return Timer()


class BaseInstrument(Instrument):
    """Instrument clss is abstract and must be implemented."""

    def __init__(
        self, name: str, unit: str = "", description: str = "", attributes: dict[str, str] | None = None
    ):
        self.name: str = name
        self.unit: str = unit
        self.description: str = description
        self.attributes: dict[str, str] | None = attributes


class CounterMap:
    """Stores Otel Counters."""

    def __init__(self, meter):
        self.meter = meter
        self.map = {}

    def clear(self) -> None:
        self.map.clear()

    def get_counter(self, name: str, attributes: dict[str, str] | None = None):
        """Returns the value of the counter or creates a new one if it does not exist."""
        key: str = name + str(attributes)
        if key in self.map.keys():
            # print("returning counter with " + key)
            return self.map[key]
        else:
            print("--> creating counter with " + key)
            # TODO:  Do Better for 63-char max
            counter = self.meter.create_up_down_counter(name if len(name) < 64 else name[:63])
            self.map[key] = counter
            return counter

    def del_counter(self, name: str, attributes: dict[str, str] | None = None) -> None:
        key: str = name + str(attributes)
        if key in self.map.keys():
            del self.map[key]


class GaugeMap:
    """Stores OTel Gauges."""

    def __init__(self, meter):
        self.meter = meter
        self.map = {}

    def clear(self) -> None:
        self.map.clear()

    def set_value(
        self,
        name: str,
        value: int | float,
        unit: str = "",
        description: str = "",
        attributes: dict[str, str] | None = None,
    ) -> None:
        """Overwrites the value of the gauge or creates a new one if it does not exist."""
        if not attributes:
            attributes = {}

        key = name + str(util.get_dict_as_key(attributes))
        self.map[key] = Measurement(value, BaseInstrument(name, unit, description), attributes)

    def get_readings(self, callback_options) -> Iterable[Measurement]:
        """Returns and clears gauge readings."""
        readings = self.poke_readings()
        self.clear()
        return readings

    def poke_readings(self) -> Iterable[Measurement]:
        """Returns gauge readings without clearing them."""
        readings = []
        for val in self.map.values():
            readings.append(val)
        return readings


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
        if conf.get("metrics", "statsd_allow_list", fallback=None):
            metrics_validator = AllowListValidator(conf.get("metrics", "statsd_allow_list"))
            if conf.get("metrics", "statsd_block_list", fallback=None):
                log.warning(
                    "Ignoring statsd_block_list as both statsd_allow_list and statsd_block_list have been set"
                )
        elif conf.get("metrics", "statsd_block_list", fallback=None):
            metrics_validator = BlockListValidator(conf.get("metrics", "statsd_block_list"))
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
        if conf.get("metrics", "statsd_allow_list", fallback=None):
            metrics_validator = AllowListValidator(conf.get("metrics", "statsd_allow_list"))
            if conf.get("metrics", "statsd_block_list", fallback=None):
                log.warning(
                    "Ignoring statsd_block_list as both statsd_allow_list and statsd_block_list have been set"
                )
        elif conf.get("metrics", "statsd_block_list", fallback=None):
            metrics_validator = BlockListValidator(conf.get("metrics", "statsd_block_list"))
        else:
            metrics_validator = AllowListValidator()
        datadog_metrics_tags = conf.getboolean("metrics", "statsd_datadog_metrics_tags", fallback=True)
        metric_tags_validator = BlockListValidator(conf.get("metrics", "statsd_disabled_tags", fallback=None))
        return SafeDogStatsdLogger(dogstatsd, metrics_validator, datadog_metrics_tags, metric_tags_validator)

    @classmethod
    def get_otel_logger(cls):
        """Get Otel logger"""
        host = conf.get("metrics", "otel_host")  # ex: "breeze-otel-collector"
        port = conf.getint("metrics", "otel_port")  # ex: 4318
        prefix = conf.get("metrics", "otel_prefix")  # ex: "airflow"
        interval = conf.getint("metrics", "otel_interval_millis")  # ex: 30000

        # TODO replace existing statsd_allow_list with metrics_allow_list??
        allow_list = conf.get("metrics", "statsd_allow_list", fallback=None)
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

        # TODO:  remove console exporter
        debug = False
        if debug:
            export_to_console = PeriodicExportingMetricReader(ConsoleMetricExporter())
            readers.append(export_to_console)

        metrics.set_meter_provider(
            MeterProvider(
                resource=resource,
                metric_readers=readers,
                shutdown_on_exit=False,
            ),
        )
        # TODO:  I like the metrics.foo() here for clarity, but maybe import these directly?

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
