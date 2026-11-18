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

import warnings

from airflow.exceptions import AirflowProviderDeprecationWarning

UPGRADE_GUIDE_URL = "https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/upgrading.html"


def warn_deprecated(message: str, *, removal_version: str) -> None:
    """
    Warn that the calling provider code ran a deprecated behaviour.

    Call this from the provider function that implements the deprecated behaviour, instead of calling
    ``warnings.warn`` there. The warning is attributed to that function's module, not to the code that
    called it: Airflow shows a ``DeprecationWarning`` only when it is attributed to an ``airflow`` module,
    so one attributed to a Dag file or to other user code is dropped by Python's default filters. That is
    why there is no ``stacklevel`` parameter.

    :param message: What is deprecated and what to use instead. The removal release and a link to the
        upgrade guide are appended.
    :param removal_version: The Amazon provider release that removes the deprecated behaviour. Pass a
        literal such as ``"10.0.0"``, so that searching for ``removal_version="10.0.0"`` lists everything
        that release has to remove.
    """
    warnings.warn(
        f"{message} The deprecated behaviour will be removed in version {removal_version} of the "
        f"Amazon provider. See {UPGRADE_GUIDE_URL}",
        AirflowProviderDeprecationWarning,
        stacklevel=2,
    )
