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

import pytest

from airflow_shared.template_rendering import render_callback_kwargs


@pytest.mark.parametrize(
    ("kwargs", "context", "expected"),
    [
        pytest.param(
            {"message": "DAG {{ dag_id }} missed deadline at {{ ds }}"},
            {"dag_id": "my_dag", "ds": "2024-06-15"},
            {"message": "DAG my_dag missed deadline at 2024-06-15"},
            id="renders_templated_string",
        ),
        pytest.param(
            {"message": "plain text", "count": 42},
            {"dag_id": "test"},
            {"message": "plain text", "count": 42},
            id="no_template_markers_is_noop",
        ),
        pytest.param(
            {"count": 5, "flag": True, "data": {"nested": "{{ dag_id }}"}},
            {"dag_id": "my_dag"},
            {"count": 5, "flag": True, "data": {"nested": "{{ dag_id }}"}},
            id="non_string_values_pass_through",
        ),
        pytest.param(
            {"context": "{{ dag_id }}", "message": "{{ dag_id }}"},
            {"dag_id": "my_dag"},
            {"context": "{{ dag_id }}", "message": "my_dag"},
            id="context_key_is_never_rendered",
        ),
        pytest.param(
            {"message": "Hello {{ nonexistent }}"},
            {"dag_id": "my_dag"},
            {"message": "Hello "},
            id="missing_key_renders_empty",
        ),
        pytest.param(
            {},
            {"dag_id": "my_dag"},
            {},
            id="empty_kwargs",
        ),
    ],
)
def test_render_callback_kwargs(kwargs, context, expected):
    assert render_callback_kwargs(kwargs, context) == expected


def test_render_callback_kwargs_bad_template_falls_back_to_raw_value():
    kwargs = {"message": "{{ this is not valid jinja"}
    assert render_callback_kwargs(kwargs, {"dag_id": "my_dag"}) == kwargs


def test_render_callback_kwargs_uses_sandboxed_environment():
    # Sandbox blocks access to unsafe attributes, matching task template rendering.
    result = render_callback_kwargs(
        {"message": "{{ x.__class__.__init__.__globals__ }}"},
        {"x": "value"},
    )
    assert result == {"message": "{{ x.__class__.__init__.__globals__ }}"}
