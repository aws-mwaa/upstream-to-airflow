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

from typing import Callable

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Instrument
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from airflow.configuration import conf
from airflow.metrics.protocols import DeltaType, Timer, TimerProtocol
from airflow.metrics.validators import AllowListValidator, validate_stat

# TODO:  Move this set into a config file with "units" and "description" values??
UP_DOWN_COUNTERS = {
    "dag_processing.processes",
}


class SafeOtelLogger:
    """Otel Logger"""

    def __init__(self, otel_provider, prefix: str = "airflow", allow_list_validator=AllowListValidator()):
        self.otel: Callable = otel_provider
        self.prefix: str = prefix
        self.metrics_validator = allow_list_validator
        self.meter = otel_provider.get_meter(__name__)
        self.metrics_map = MetricsMap(self.meter)

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
            raise ValueError("count and rate must both be positive values.")
        # TODO: I don't think this is the right use for rate???
        value = count * rate

        if self.metrics_validator.test(stat):
            counter = self.metrics_map.get_counter(f"{self.prefix}.{stat}")
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
            counter = self.metrics_map.get_counter(f"{self.prefix}.{stat}")
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


def is_up_down_counter(name):
    # Append the `airflow` prefix to all names
    return name in {f"airflow.{name}" for name in UP_DOWN_COUNTERS}


class BaseInstrument(Instrument):
    """Instrument clss is abstract and must be implemented."""

    def __init__(
        self, name: str, unit: str = "", description: str = "", attributes: dict[str, str] | None = None
    ):
        self.name: str = name
        self.unit: str = unit
        self.description: str = description
        self.attributes: dict[str, str] | None = attributes


class MetricsMap:
    """Stores Otel Counters."""

    def __init__(self, meter):
        self.meter = meter
        self.map = {}

    def clear(self) -> None:
        self.map.clear()

    def _create_counter(self, name):
        """Creates a new counter or up_down_counter for the provided name."""
        # TODO:  OTel imposes a 63-character limit to metric names.
        #   Any part of the name which is not fixed should instead be a tag.
        #   error message here:  https://quip-amazon.com/qA0wAEzAY7gn/Notes-OTel-Planning#temp:C:QHV87f5f9b6338e4085935c14e93
        counter_name = name if len(name) < 64 else name[:63]

        if is_up_down_counter(name):
            counter = self.meter.create_up_down_counter(counter_name)
        else:
            counter = self.meter.create_counter(counter_name)

        print(f"--> created {name} as a {type(counter)}")
        return counter

    def get_counter(self, name: str, attributes: dict[str, str] | None = None):
        """
        Returns the value of the counter; creates a new one if it does not exist.

        :param name: The name of the counter to fetch or create.
        :param attributes:  Counter attributes, used to generate a unique key to store the counter.
        """
        key: str = name + str(attributes)
        if key in self.map.keys():
            return self.map[key]
        else:
            new_counter = self._create_counter(name)
            self.map[key] = new_counter
            return new_counter

    def del_counter(self, name: str, attributes: dict[str, str] | None = None) -> None:
        """
        Deletes a counter.

        :param name: The name of the counter to fetch or create.
        :param attributes: Counter attributes which were used to generate a unique key to store the counter.
        """
        key: str = name + str(attributes)
        if key in self.map.keys():
            del self.map[key]


def get_otel_logger(cls) -> SafeOtelLogger:
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
