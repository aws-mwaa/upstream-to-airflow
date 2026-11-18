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

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.utils import return_values
from airflow.providers.amazon.aws.utils.return_values import should_use_10_0_return_value
from airflow.providers.common.compat.sdk import BaseOperator

from tests_common.test_utils.config import conf_vars

NEW_VALUE = "the resource as a dict"


class ReturningOperator(BaseOperator):
    def execute(self, context):
        return should_use_10_0_return_value(self, new_value=NEW_VALUE)


@pytest.mark.parametrize("do_xcom_push", [True, False])
@conf_vars({("aws", "use_10_0_return_values"): "True"})
def test_option_on_returns_the_10_0_value_without_warning(do_xcom_push):
    operator = ReturningOperator(task_id="task", do_xcom_push=do_xcom_push)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert operator.execute({}) is True


def test_option_off_returns_the_current_value_without_warning_when_nothing_is_pushed():
    operator = ReturningOperator(task_id="task", do_xcom_push=False)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert operator.execute({}) is False


def test_option_off_warns_when_the_value_is_pushed():
    operator = ReturningOperator(task_id="task")

    with pytest.warns(AirflowProviderDeprecationWarning) as recorded:
        assert operator.execute({}) is False

    (warning,) = recorded
    message = str(warning.message)
    assert (
        f"ReturningOperator returns is deprecated. The operator will return {NEW_VALUE} instead." in message
    )
    assert "[aws] use_10_0_return_values" in message
    assert "removed in version 10.0.0 of the Amazon provider" in message
    # Attributed to the provider, so that Airflow's warning filters show it for an operator
    # defined outside the ``airflow`` package, such as this one.
    assert warning.filename == return_values.__file__
