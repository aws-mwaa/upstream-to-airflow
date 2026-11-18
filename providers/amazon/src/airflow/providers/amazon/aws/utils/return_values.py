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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.utils.deprecation import warn_deprecated
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import BaseOperator


def should_use_10_0_return_value(operator: BaseOperator, *, new_value: str) -> bool:
    """
    Decide whether an operator returns the value it returns in version 10.0.0 of the Amazon provider.

    Call this from ``execute()`` or ``execute_complete()`` of an operator whose return value changes
    in 10.0.0 in a way that cannot be made additive. Hook methods must not read the option: other
    providers and plugins that call them cannot set a deployment option.

    With ``[aws] use_10_0_return_values`` off, the operator keeps returning the value it returns
    today. The deprecation warning is only emitted when the task pushes that value to XCom, because a
    task that pushes nothing has nothing to migrate.

    :param operator: The operator that is about to return.
    :param new_value: What the operator returns in 10.0.0, worded to follow "returns".
    """
    if conf.getboolean("aws", "use_10_0_return_values", fallback=False):
        return True
    if operator.do_xcom_push:
        warn_deprecated(
            f"The value {type(operator).__name__} returns is deprecated. The operator will return "
            f"{new_value} instead. To return the new value now, set the [aws] use_10_0_return_values "
            "option to True wherever tasks run, once everything that reads this XCom handles it. A "
            "task whose return value is not used can set do_xcom_push=False.",
            removal_version="10.0.0",
        )
    return False
