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

import importlib.util
import warnings
from typing import TYPE_CHECKING

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.utils.deprecation import UPGRADE_GUIDE_URL, warn_deprecated

if TYPE_CHECKING:
    from pathlib import Path
    from types import ModuleType

PROVIDER_SOURCE = """
from airflow.providers.amazon.aws.utils.deprecation import warn_deprecated
from airflow.providers.common.compat.sdk import BaseOperator


class ProbeOperator(BaseOperator):
    def __init__(self, *, old=None, **kwargs):
        super().__init__(**kwargs)
        if old is not None:
            warn_deprecated("`old` is deprecated. Use `new` instead.", removal_version="10.0.0")


def probe_hook_method():
    warn_deprecated("`probe_hook_method` is deprecated.", removal_version="10.0.0")
"""

USER_SOURCE = """
def call(deprecated, **kwargs):
    return deprecated(**kwargs)
"""


def load_module(name: str, path: Path, source: str) -> ModuleType:
    path.write_text(source)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec
    assert spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_warn_deprecated_names_removal_release_and_upgrade_guide():
    with pytest.warns(AirflowProviderDeprecationWarning) as recorded:
        warn_deprecated("`old` is deprecated. Use `new` instead.", removal_version="10.0.0")

    assert [str(w.message) for w in recorded] == [
        "`old` is deprecated. Use `new` instead. The deprecated behaviour will be removed in version "
        f"10.0.0 of the Amazon provider. See {UPGRADE_GUIDE_URL}"
    ]


@pytest.mark.parametrize(
    ("deprecated", "kwargs"),
    [
        pytest.param("ProbeOperator", {"task_id": "probe", "old": "value"}, id="operator_param"),
        pytest.param("probe_hook_method", {}, id="hook_method"),
    ],
)
def test_warning_triggered_from_user_code_passes_airflow_default_filters(tmp_path, deprecated, kwargs):
    provider = load_module(
        "airflow.providers.amazon.aws.deprecation_probe", tmp_path / "deprecation_probe.py", PROVIDER_SOURCE
    )
    # Named the way the Dag importer names a Dag file's module.
    user_code = load_module("unusual_prefix_0123_my_dag", tmp_path / "my_dag.py", USER_SOURCE)

    with warnings.catch_warnings(record=True) as recorded:
        warnings.resetwarnings()
        # Python ignores DeprecationWarning outside ``__main__``, and Airflow turns it back on for
        # ``airflow`` modules only.
        warnings.simplefilter("ignore", DeprecationWarning)
        warnings.filterwarnings("default", category=DeprecationWarning, module="airflow")
        user_code.call(getattr(provider, deprecated), **kwargs)

    assert [(w.category, w.filename) for w in recorded] == [
        (AirflowProviderDeprecationWarning, provider.__file__)
    ]
