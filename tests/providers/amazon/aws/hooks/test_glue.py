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

import json
from unittest import mock

import boto3
import pytest
from moto import mock_glue, mock_iam

from airflow.providers.amazon.aws.hooks.glue import GlueJobHook


@pytest.fixture
def mock_conn():
    with mock.patch.object(GlueJobHook, "get_conn") as _get_conn:
        yield _get_conn.return_value


class TestGlueJobHook:
    def setup_method(self):
        self.some_aws_region = "us-west-2"

    @mock_iam
    @pytest.mark.parametrize("role_path", ["/", "/custom-path/"])
    def test_get_iam_execution_role(self, role_path):
        expected_role = "my_test_role"
        boto3.client("iam").create_role(
            Path=role_path,
            RoleName=expected_role,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": {
                        "Effect": "Allow",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    },
                }
            ),
        )

        hook = GlueJobHook(
            aws_conn_id=None,
            job_name="aws_test_glue_job",
            s3_bucket="some_bucket",
            iam_role_name=expected_role,
        )
        iam_role = hook.get_iam_execution_role()
        assert iam_role is not None
        assert "Role" in iam_role
        assert "Arn" in iam_role["Role"]
        assert iam_role["Role"]["Arn"] == f"arn:aws:iam::123456789012:role{role_path}{expected_role}"

    def test_get_or_create_glue_job_get_existing_job(self, mock_conn):
        """
        Calls 'get_or_create_glue_job' with a existing job.
        Should retrieve existing one.
        """
        expected_job_name = "simple-job"
        mock_conn.get_job.return_value = {"Job": {"Name": expected_job_name}}

        hook = GlueJobHook(
            job_name=expected_job_name,
            desc="This is test case job from Airflow",
            script_location="s3://bucket",
            s3_bucket="bucket",
        )

        result = hook.get_or_create_glue_job()

        mock_conn.get_job.assert_called_once_with(JobName=hook.job_name)
        assert result == expected_job_name

    @mock_glue
    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    def test_get_or_create_glue_job_create_new_job(self, mock_get_iam_execution_role):
        """
        Calls 'get_or_create_glue_job' with no existing job.
        Should create a new job.
        """
        mock_get_iam_execution_role.return_value = {"Role": {"RoleName": "my_test_role", "Arn": "test_role"}}
        expected_job_name = "aws_test_glue_job"

        hook = GlueJobHook(
            job_name=expected_job_name,
            desc="This is test case job from Airflow",
            script_location="s3://bucket",
            s3_bucket="bucket",
        )

        result = hook.get_or_create_glue_job()

        assert result == expected_job_name

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    def test_get_or_create_glue_job_worker_type(self, mock_get_iam_execution_role, mock_conn):
        mock_get_iam_execution_role.return_value = mock.MagicMock(Role={"RoleName": "my_test_role"})

        mock_glue_job = mock_conn.get_job()["Job"]["Name"]
        glue_job = GlueJobHook(
            create_job_kwargs={"WorkerType": "G.2X", "NumberOfWorkers": 60}
        ).get_or_create_glue_job()
        assert glue_job == mock_glue_job

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    def test_init_worker_type_value_error(self, mock_get_iam_execution_role):
        mock_get_iam_execution_role.return_value = mock.MagicMock(Role={"RoleName": "my_test_role"})

        with pytest.raises(ValueError, match="Cannot specify num_of_dpus with custom WorkerType"):
            GlueJobHook(
                num_of_dpus=20,
                create_job_kwargs={"WorkerType": "G.2X", "NumberOfWorkers": 60},
            )

    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "get_or_create_glue_job")
    def test_initialize_job(self, mock_get_or_create_glue_job, mock_get_job_state, mock_conn):
        mock_get_or_create_glue_job.Name = mock.Mock(Name="aws_test_glue_job")
        mock_conn.start_job_run()
        mock_job_run_state = mock_get_job_state.return_value
        hook = GlueJobHook()

        glue_job_run = hook.initialize_job()
        glue_job_run_state = hook.get_job_state(glue_job_run["JobName"], glue_job_run["JobRunId"])

        assert glue_job_run_state == mock_job_run_state
