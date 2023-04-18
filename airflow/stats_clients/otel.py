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

from typing import Iterable

from opentelemetry.metrics import Instrument
from opentelemetry.sdk.metrics._internal.measurement import Measurement
from opentelemetry.sdk import util

# TODO:  Move this set into a config file with "units" and "description" values??
UP_DOWN_COUNTERS = {
    "dag_processing.processes",
}


def is_up_down_counter(name):
    # Append the `airflow` prefix to all names
    return name in {f"airflow.{name}" for name in UP_DOWN_COUNTERS}


class BaseInstrument(Instrument):
    """Instrument class is abstract and must be implemented."""

    def __init__(
        self, name: str, unit: str = "", description: str = "", attributes: dict[str, str] | None = None
    ):
        self.name: str = name
        self.unit: str = unit
        self.description: str = description
        self.attributes: dict[str, str] | None = attributes


class GaugeMap:
    """Stores Otel Gauges."""

    def __init__(self, meter):
        self.meter = meter
        self.map = {}

    def clear(self) -> None:
        self.map.clear()

    # set value would store the value of the gauge
    def set_value(self, name, value, unit="", description="", attributes=None) -> None:
        if not attributes:
            attributes = {}

        key = name + str(util.get_dict_as_key(attributes))
        # any previously existing measurement would get effectively overwritten
        self.map[key] = Measurement(value, BaseInstrument(name, unit, description), attributes)

    # retrieve readings
    def get_readings(self) -> Iterable[Measurement]:
        ret = self.poke_readings()
        # clear the map when getting the readings
        # in this way, any accumulated gauge wouldn't survive
        # once the readings are extracted.
        self.clear()
        return ret

    # poke readings, without clearing the gauge map
    def poke_readings(self) -> Iterable[Measurement]:
        ret = []
        for val in self.map.values():
            ret.append(val)
        return ret


class CounterMap:
    """Stores Otel Counters."""

    def __init__(self, meter):
        self.meter = meter
        self.map = {}

    def clear(self) -> None:
        self.map.clear()

    def _create_counter(self, name):
        """Creates a new counter or up_down_counter for the provided name."""
        # TODO:  Find out where the 63-char limit comes from and handle this better.
        #        Maybe store as UUID(name) ??
        #        error messsage here:  https://quip-amazon.com/qA0wAEzAY7gn/Notes-OTel-Planning#temp:C:QHV87f5f9b6338e4085935c14e93
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
