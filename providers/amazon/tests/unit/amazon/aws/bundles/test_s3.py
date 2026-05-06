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

import os
from unittest.mock import MagicMock, call

import boto3
import pytest
from moto import mock_aws

import airflow.version
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import AirflowException

from tests_common.test_utils.config import conf_vars

AWS_CONN_ID_WITH_REGION = "s3_dags_connection"
AWS_CONN_ID_REGION = "eu-central-1"
AWS_CONN_ID_DEFAULT = "aws_default"
S3_BUCKET_NAME = "my-airflow-dags-bucket"
S3_BUCKET_PREFIX = "project1/dags"

if airflow.version.version.strip().startswith("3"):
    from airflow.providers.amazon.aws.bundles.s3 import S3DagBundle


@pytest.fixture
def mocked_s3_resource():
    with mock_aws():
        yield boto3.resource("s3")


@pytest.fixture
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture
def s3_bucket(mocked_s3_resource, s3_client):
    bucket = mocked_s3_resource.create_bucket(Bucket=S3_BUCKET_NAME)

    s3_client.put_object(Bucket=bucket.name, Key=S3_BUCKET_PREFIX + "/dag_01.py", Body=b"test data")
    s3_client.put_object(Bucket=bucket.name, Key=S3_BUCKET_PREFIX + "/dag_02.py", Body=b"test data")
    s3_client.put_object(
        Bucket=bucket.name, Key=S3_BUCKET_PREFIX + "/subproject1/dag_a.py", Body=b"test data"
    )
    s3_client.put_object(
        Bucket=bucket.name, Key=S3_BUCKET_PREFIX + "/subproject1/dag_b.py", Body=b"test data"
    )

    return bucket


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


@pytest.mark.skipif(not airflow.version.version.strip().startswith("3"), reason="Airflow >=3.0.0 test")
class TestS3DagBundle:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=AWS_CONN_ID_DEFAULT,
                conn_type="aws",
                extra={
                    "config_kwargs": {"s3": {"bucket_name": S3_BUCKET_NAME}},
                },
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=AWS_CONN_ID_WITH_REGION,
                conn_type="aws",
                extra={
                    "config_kwargs": {"s3": {"bucket_name": S3_BUCKET_NAME}},
                    "region_name": AWS_CONN_ID_REGION,
                },
            )
        )

    def test_view_url_generates_presigned_url(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1/dags", bucket_name=S3_BUCKET_NAME
        )

        url: str = bundle.view_url("test_version")
        assert url.startswith("https://my-airflow-dags-bucket.s3.amazonaws.com/project1/dags")

    def test_view_url_template_generates_presigned_url(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1/dags", bucket_name=S3_BUCKET_NAME
        )
        url: str = bundle.view_url_template()
        assert url.startswith("https://my-airflow-dags-bucket.s3.amazonaws.com/project1/dags")

    def test_supports_versioning(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1/dags", bucket_name=S3_BUCKET_NAME
        )
        assert S3DagBundle.supports_versioning is True

    def test_correct_bundle_path_used(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1_dags", bucket_name="airflow_dags"
        )
        assert str(bundle.base_dir) == str(bundle.s3_dags_dir)

    def test_s3_bucket_and_prefix_validated(self, s3_bucket):
        hook = S3Hook(aws_conn_id=AWS_CONN_ID_DEFAULT)
        assert hook.check_for_bucket(s3_bucket.name) is True

        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix="project1_dags",
            bucket_name="non-existing-bucket",
        )
        with pytest.raises(AirflowException, match="S3 bucket.*non-existing-bucket.*does not exist.*"):
            bundle.initialize()

        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix="non-existing-prefix",
            bucket_name=S3_BUCKET_NAME,
        )
        with pytest.raises(AirflowException, match="S3 prefix.*non-existing-prefix.*does not exist.*"):
            bundle.initialize()

        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix=S3_BUCKET_PREFIX,
            bucket_name=S3_BUCKET_NAME,
        )
        # initialize succeeds, with correct prefix and bucket
        bundle.initialize()
        assert bundle.s3_hook.region_name == AWS_CONN_ID_REGION

        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix="",
            bucket_name=S3_BUCKET_NAME,
        )
        # initialize succeeds, with empty prefix
        bundle.initialize()
        assert bundle.s3_hook.region_name == AWS_CONN_ID_REGION

    def _upload_fixtures(self, bucket: str, fixtures_dir: str) -> None:
        client = boto3.client("s3")
        fixtures_paths = [
            os.path.join(path, filename) for path, _, files in os.walk(fixtures_dir) for filename in files
        ]
        for path in fixtures_paths:
            key = os.path.relpath(path, fixtures_dir)
            client.upload_file(Filename=path, Bucket=bucket, Key=key)

    def test_refresh(self, s3_bucket, s3_client):
        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix=S3_BUCKET_PREFIX,
            bucket_name=S3_BUCKET_NAME,
        )
        bundle._log.debug = MagicMock()
        # Create a pytest Call object to compare against the call_args_list of the _log.debug mock
        download_log_call = call(
            "Downloading Dags from s3://%s/%s to %s", S3_BUCKET_NAME, S3_BUCKET_PREFIX, bundle.s3_dags_dir
        )
        bundle.initialize()
        assert bundle._log.debug.call_count == 1
        assert bundle._log.debug.call_args_list == [download_log_call]
        bundle.refresh()
        assert bundle._log.debug.call_count == 2
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call]
        bundle.refresh()
        assert bundle._log.debug.call_count == 3
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call, download_log_call]

    def test_refresh_without_prefix(self, s3_bucket, s3_client):
        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            bucket_name=S3_BUCKET_NAME,
        )
        bundle._log.debug = MagicMock()
        download_log_call = call(
            "Downloading Dags from s3://%s/%s to %s", S3_BUCKET_NAME, "", bundle.s3_dags_dir
        )
        assert bundle.prefix == ""
        bundle.initialize()
        bundle.refresh()
        assert bundle._log.debug.call_count == 2
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call]

    def test_get_current_version_with_versioned_bucket(self, s3_client):
        """Test manifest generation with a versioned S3 bucket."""
        bucket_name = "versioned-bucket"
        s3_client.create_bucket(Bucket=bucket_name)
        # Enable versioning
        s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        s3_client.put_object(Bucket=bucket_name, Key="dags/dag1.py", Body=b"print('dag1')")
        s3_client.put_object(Bucket=bucket_name, Key="dags/dag2.py", Body=b"print('dag2')")

        bundle = S3DagBundle(
            name="test_versioned", aws_conn_id=AWS_CONN_ID_DEFAULT, bucket_name=bucket_name, prefix="dags"
        )
        result = bundle.get_current_version()

        assert result is not None
        assert result.version  # SHA-256 hex string
        assert len(result.version) == 64  # SHA-256 produces 64 hex chars
        assert result.data is not None
        assert result.data["schema_version"] == 1
        assert "dags/dag1.py" in result.data["files"]
        assert "dags/dag2.py" in result.data["files"]

    def test_get_current_version_deterministic(self, s3_client):
        """Same bucket state produces the same hash."""
        bucket_name = "deterministic-bucket"
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        s3_client.put_object(Bucket=bucket_name, Key="dag.py", Body=b"content")

        bundle = S3DagBundle(name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, bucket_name=bucket_name, prefix="")
        v1 = bundle.get_current_version()
        bundle._cached_bundle_version = None  # Clear cache
        v2 = bundle.get_current_version()

        assert v1.version == v2.version
        assert v1.data == v2.data

    def test_get_current_version_unversioned_bucket(self, s3_client):
        """Unversioned bucket returns None with a warning."""
        bucket_name = "unversioned-bucket"
        s3_client.create_bucket(Bucket=bucket_name)
        # No versioning enabled
        s3_client.put_object(Bucket=bucket_name, Key="dag.py", Body=b"content")

        bundle = S3DagBundle(name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, bucket_name=bucket_name, prefix="")
        result = bundle.get_current_version()

        assert result is None

    def test_get_current_version_caches_result(self, s3_client):
        """get_current_version caches the result within a refresh cycle."""
        bucket_name = "cached-bucket"
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        s3_client.put_object(Bucket=bucket_name, Key="dag.py", Body=b"content")

        bundle = S3DagBundle(name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, bucket_name=bucket_name, prefix="")
        v1 = bundle.get_current_version()
        v2 = bundle.get_current_version()

        assert v1 is v2  # Same object, not just equal

    def test_refresh_clears_cache(self, s3_bucket):
        """refresh() clears the cached version so next call rebuilds manifest."""
        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_DEFAULT,
            bucket_name=S3_BUCKET_NAME,
            prefix=S3_BUCKET_PREFIX,
        )
        bundle._cached_bundle_version = MagicMock()
        bundle.refresh()
        assert bundle._cached_bundle_version is None

    def test_sync_versioned_writes_files(self, s3_client, bundle_temp_dir):
        """_sync_versioned fetches specific object versions and writes them locally."""
        bucket_name = "sync-versioned-bucket"
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        resp1 = s3_client.put_object(Bucket=bucket_name, Key="dags/dag1.py", Body=b"dag1 content")
        resp2 = s3_client.put_object(Bucket=bucket_name, Key="dags/dag2.py", Body=b"dag2 content")

        manifest = {
            "schema_version": 1,
            "files": {
                "dags/dag1.py": resp1["VersionId"],
                "dags/dag2.py": resp2["VersionId"],
            },
        }

        bundle = S3DagBundle(
            name="test_sync",
            aws_conn_id=AWS_CONN_ID_DEFAULT,
            bucket_name=bucket_name,
            prefix="dags",
            version="testhash",
            version_data=manifest,
        )
        bundle._sync_versioned(manifest)

        assert (bundle.s3_dags_dir / "dag1.py").read_bytes() == b"dag1 content"
        assert (bundle.s3_dags_dir / "dag2.py").read_bytes() == b"dag2 content"

    def test_sync_versioned_unsupported_schema_version(self, s3_bucket):
        """_sync_versioned raises for unrecognized schema_version."""
        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_DEFAULT,
            bucket_name=S3_BUCKET_NAME,
            prefix=S3_BUCKET_PREFIX,
        )
        manifest = {"schema_version": 99, "files": {}}

        with pytest.raises(AirflowException, match="schema version 99 is not supported"):
            bundle._sync_versioned(manifest)

    def test_versioned_bundle_uses_versions_dir(self, s3_client, bundle_temp_dir):
        """When version is set, s3_dags_dir uses versions_dir/<version>."""
        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_DEFAULT,
            bucket_name="bucket",
            prefix="",
            version="abc123",
        )
        assert "versions" in str(bundle.s3_dags_dir)
        assert "abc123" in str(bundle.s3_dags_dir)
