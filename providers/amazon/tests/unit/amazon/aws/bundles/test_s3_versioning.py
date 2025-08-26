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

import hashlib
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.bundles.s3 import S3DagBundle

from tests_common.test_utils.config import conf_vars

# Skip if running on Airflow < 3.0
airflow = pytest.importorskip("airflow", minversion="3.0.0")

pytestmark = [pytest.mark.db_test, pytest.mark.skipif("airflow.version.version < '3.0.0'")]


class TestS3DagBundleVersioning:
    """Test S3 DAG bundle versioning functionality."""

    @pytest.fixture(autouse=True)
    def bundle_temp_dir(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            yield tmp_path

    @pytest.fixture
    def mock_s3_hook(self):
        """Mock S3Hook for testing."""
        with patch("airflow.providers.amazon.aws.bundles.s3.S3Hook") as mock_hook_class:
            mock_hook = Mock()
            mock_hook_class.return_value = mock_hook
            mock_hook.check_for_bucket.return_value = True
            mock_hook.check_for_prefix.return_value = True
            mock_hook.region_name = "us-east-1"
            
            # Mock connection
            mock_conn = Mock()
            mock_hook.get_conn.return_value = mock_conn
            mock_hook.conn = mock_conn
            
            yield mock_hook

    def test_versioning_disabled_by_default(self):
        """Test that versioning is disabled by default."""
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            enable_versioning=False,
        )
        
        assert bundle.supports_versioning is False
        assert bundle.supports_manifest_versioning is False
        assert bundle.enable_versioning is False

    def test_versioning_enabled_but_not_validated_until_init(self):
        """Test that versioning is provisionally enabled but validated during init."""
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            enable_versioning=True,
        )
        
        # Before initialization, manifest versioning is provisionally enabled
        assert bundle.enable_versioning is True
        assert bundle.supports_manifest_versioning is True
        # But regular versioning is not set until validation
        assert bundle.supports_versioning is False

    def test_initialization_with_versioning_enabled_and_bucket_supports_it(self, mock_s3_hook):
        """Test initialization when versioning is enabled and bucket supports it."""
        # Mock bucket versioning check
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Enabled"}
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            enable_versioning=True,
        )
        
        bundle.initialize()
        
        # After initialization, both should be enabled
        assert bundle.supports_versioning is True
        assert bundle.supports_manifest_versioning is True

    def test_initialization_with_versioning_enabled_but_bucket_doesnt_support_it(self, mock_s3_hook):
        """Test initialization when versioning is enabled but bucket doesn't support it."""
        # Mock bucket versioning check - not enabled
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Suspended"}
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            enable_versioning=True,
        )
        
        # Should log warning and fall back to non-versioned behavior
        bundle.initialize()
        
        assert bundle.supports_versioning is False
        assert bundle.supports_manifest_versioning is True  # Still provisionally enabled

    def test_initialization_fails_when_specific_version_requested_but_bucket_not_versioned(self, mock_s3_hook):
        """Test that initialization fails when specific version is requested but bucket doesn't support versioning."""
        # Mock bucket versioning check - not enabled
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {}
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            version="some-version",  # Specific version requested
            enable_versioning=True,
        )
        
        with pytest.raises(AirflowException, match="S3 bundle versioning requires bucket.*to have versioning enabled"):
            bundle.initialize()

    def test_create_s3_manifest_success(self, mock_s3_hook):
        """Test successful S3 manifest creation."""
        # Setup mock responses
        mock_objects = ["dags/dag1.py", "dags/dag2.py", "dags/utils.py"]
        mock_s3_hook.list_keys.return_value = mock_objects
        
        # Mock head_object responses for initial scan
        mock_head_responses = {
            "dags/dag1.py": {
                "ETag": '"abc123"',
                "LastModified": "2024-01-01T10:00:00Z",
                "ContentLength": 1234,
                "VersionId": "v1",
            },
            "dags/dag2.py": {
                "ETag": '"def456"',
                "LastModified": "2024-01-01T10:05:00Z", 
                "ContentLength": 5678,
                "VersionId": "v2",
            },
            "dags/utils.py": {
                "ETag": '"ghi789"',
                "LastModified": "2024-01-01T10:10:00Z",
                "ContentLength": 999,
                "VersionId": "v3",
            },
        }
        
        def head_object_side_effect(*args, **kwargs):
            key = kwargs.get("Key") or args[1]
            return mock_head_responses[key]
        
        mock_s3_hook.conn.head_object.side_effect = head_object_side_effect
        
        # Mock bucket versioning as enabled
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Enabled"}
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket", 
            prefix="dags/",
            enable_versioning=True,
        )
        bundle.initialize()
        
        # Create manifest
        manifest = bundle._create_s3_manifest()
        
        # Verify manifest structure
        assert manifest["bucket"] == "test-bucket"
        assert manifest["prefix"] == "dags/"
        assert manifest["manifest_version"] == "1.0"
        assert "snapshot_time" in manifest
        
        # Verify objects
        objects = manifest["objects"]
        assert len(objects) == 3
        
        assert objects["dags/dag1.py"]["etag"] == "abc123"
        assert objects["dags/dag1.py"]["version_id"] == "v1"
        assert objects["dags/dag1.py"]["size"] == 1234
        
        assert objects["dags/dag2.py"]["etag"] == "def456"
        assert objects["dags/dag2.py"]["version_id"] == "v2"

    def test_create_s3_manifest_with_validation_failure_then_retry_success(self, mock_s3_hook):
        """Test manifest creation with validation failure that succeeds on retry."""
        mock_objects = ["dags/dag1.py"]
        mock_s3_hook.list_keys.return_value = mock_objects
        
        # Mock bucket versioning
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Enabled"}
        
        # Counter to track calls
        call_count = 0
        
        def head_object_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            
            # First scan: return initial ETag
            # Validation scan (first attempt): return different ETag (validation failure)
            # Second scan: return consistent ETag
            # Validation scan (second attempt): return same ETag (validation success)
            if call_count in [1, 3, 4]:  # Initial scan + second attempt
                return {
                    "ETag": '"consistent123"',
                    "LastModified": "2024-01-01T10:00:00Z",
                    "ContentLength": 1234,
                    "VersionId": "v1",
                }
            else:  # Validation scan first attempt - different ETag
                return {
                    "ETag": '"changed456"',
                    "LastModified": "2024-01-01T10:00:00Z", 
                    "ContentLength": 1234,
                    "VersionId": "v1",
                }
        
        mock_s3_hook.conn.head_object.side_effect = head_object_side_effect
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            prefix="dags/",
            enable_versioning=True,
        )
        bundle.initialize()
        
        with patch("time.sleep"):  # Speed up retries
            manifest = bundle._create_s3_manifest()
        
        # Should succeed on retry
        assert manifest["objects"]["dags/dag1.py"]["etag"] == "consistent123"
        assert call_count == 4  # 2 attempts * 2 calls each (scan + validation)

    def test_create_s3_manifest_max_retries_exceeded(self, mock_s3_hook):
        """Test manifest creation fails after max retries due to ongoing changes."""
        mock_objects = ["dags/dag1.py"]
        mock_s3_hook.list_keys.return_value = mock_objects
        
        # Mock bucket versioning
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Enabled"}
        
        # Always return different ETags to simulate constant changes
        call_count = 0
        def head_object_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return {
                "ETag": f'"changing-{call_count}"',
                "LastModified": "2024-01-01T10:00:00Z",
                "ContentLength": 1234,
                "VersionId": "v1",
            }
        
        mock_s3_hook.conn.head_object.side_effect = head_object_side_effect
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            prefix="dags/",
            enable_versioning=True,
        )
        bundle.initialize()
        
        with (
            patch("time.sleep"),  # Speed up retries
            pytest.raises(AirflowException, match="Could not create consistent S3 manifest after .* attempts"),
        ):
            bundle._create_s3_manifest()

    def test_get_current_version_returns_deterministic_hash(self, mock_s3_hook):
        """Test that get_current_version returns deterministic hash based on manifest."""
        mock_s3_hook.list_keys.return_value = ["dags/dag1.py"]
        mock_s3_hook.conn.head_object.return_value = {
            "ETag": '"abc123"',
            "LastModified": "2024-01-01T10:00:00Z",
            "ContentLength": 1234,
            "VersionId": "v1",
        }
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Enabled"}
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            prefix="dags/",
            enable_versioning=True,
        )
        bundle.initialize()
        
        # Get version twice - should be consistent
        version1 = bundle.get_current_version()
        version2 = bundle.get_current_version()
        
        assert version1 == version2
        assert version1.startswith("s3-")
        assert len(version1) == 19  # "s3-" + 16 hex chars

    def test_get_current_version_returns_none_when_versioning_disabled(self):
        """Test that get_current_version returns None when versioning is disabled."""
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            enable_versioning=False,
        )
        
        version = bundle.get_current_version()
        assert version is None

    def test_recreate_version_from_manifest(self, mock_s3_hook, tmp_path):
        """Test recreating a version from stored manifest."""
        # Setup bundle
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Enabled"}
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            prefix="dags/",
            enable_versioning=True,
        )
        bundle.initialize()
        
        # Store a manifest in the database
        version_id = "test-version-123"
        manifest_data = {
            "bucket": "test-bucket",
            "prefix": "dags/",
            "objects": {
                "dags/dag1.py": {
                    "etag": "abc123",
                    "version_id": "v1",
                    "size": 1234,
                },
                "dags/subdir/dag2.py": {
                    "etag": "def456",
                    "version_id": "v2", 
                    "size": 5678,
                },
            },
        }
        
        bundle.store_version_manifest(version_id, manifest_data)
        
        # Mock download_file
        download_calls = []
        def mock_download_file(**kwargs):
            download_calls.append(kwargs)
            # Create the file so path checks pass
            Path(kwargs["Filename"]).parent.mkdir(parents=True, exist_ok=True)
            Path(kwargs["Filename"]).write_text("mock content")
        
        mock_s3_hook.conn.download_file.side_effect = mock_download_file
        
        # Recreate version
        target_path = tmp_path / "recreated"
        bundle.recreate_version_from_manifest(version_id, target_path)
        
        # Verify downloads
        assert len(download_calls) == 2
        
        # Check first file download
        call1 = download_calls[0]
        assert call1["Bucket"] == "test-bucket"
        assert call1["Key"] == "dags/dag1.py"
        assert call1["VersionId"] == "v1"
        assert call1["Filename"] == str(target_path / "dag1.py")
        
        # Check second file download (in subdirectory)
        call2 = download_calls[1] 
        assert call2["Bucket"] == "test-bucket"
        assert call2["Key"] == "dags/subdir/dag2.py"
        assert call2["VersionId"] == "v2"
        assert call2["Filename"] == str(target_path / "subdir" / "dag2.py")

    def test_recreate_version_from_manifest_missing_version_id(self, mock_s3_hook, tmp_path):
        """Test recreating version when manifest has no version_id (fallback behavior)."""
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Enabled"}
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            prefix="dags/",
            enable_versioning=True,
        )
        bundle.initialize()
        
        # Manifest without version_id (older format)
        version_id = "test-version-no-vid"
        manifest_data = {
            "bucket": "test-bucket", 
            "prefix": "dags/",
            "objects": {
                "dags/dag1.py": {
                    "etag": "abc123",
                    # No version_id field
                    "size": 1234,
                },
            },
        }
        
        bundle.store_version_manifest(version_id, manifest_data)
        
        # Mock download_file
        download_calls = []
        def mock_download_file(**kwargs):
            download_calls.append(kwargs)
            Path(kwargs["Filename"]).parent.mkdir(parents=True, exist_ok=True)
            Path(kwargs["Filename"]).write_text("mock content")
        
        mock_s3_hook.conn.download_file.side_effect = mock_download_file
        
        # Recreate version
        target_path = tmp_path / "recreated"
        bundle.recreate_version_from_manifest(version_id, target_path)
        # TODO: Should this be a failure case?
        # Should still download, but without VersionId parameter
        assert len(download_calls) == 1
        call = download_calls[0]
        assert call["Bucket"] == "test-bucket"
        assert call["Key"] == "dags/dag1.py"
        assert "VersionId" not in call  # No version ID since not in manifest

    def test_recreate_version_from_manifest_download_error(self, mock_s3_hook, tmp_path):
        """Test that download errors during recreation are properly handled."""
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Enabled"}
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            enable_versioning=True,
        )
        bundle.initialize()
        
        # Store manifest
        version_id = "test-version-error"
        manifest_data = {
            "bucket": "test-bucket",
            "prefix": "",
            "objects": {
                "bad-file.py": {"etag": "abc123", "version_id": "v1"},
            },
        }
        bundle.store_version_manifest(version_id, manifest_data)
        
        # Mock download failure
        mock_s3_hook.conn.download_file.side_effect = Exception("S3 download failed")
        
        target_path = tmp_path / "recreated"
        
        with pytest.raises(AirflowException, match="Failed to recreate version.*could not download"):
            bundle.recreate_version_from_manifest(version_id, target_path)

    def test_refresh_uses_manifest_when_versioned(self, mock_s3_hook):
        """Test that refresh uses manifest recreation for versioned bundles."""
        mock_s3_hook.get_conn.return_value.get_bucket_versioning.return_value = {"Status": "Enabled"}
        
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            version="some-version",
            enable_versioning=True,
        )
        
        # Mock the recreate method
        with patch.object(bundle, 'recreate_version_from_manifest') as mock_recreate:
            bundle.refresh()
            
            # Should call recreate instead of sync_to_local_dir
            mock_recreate.assert_called_once_with("some-version", bundle.s3_dags_dir)
            mock_s3_hook.sync_to_local_dir.assert_not_called()

    def test_refresh_uses_sync_when_not_versioned(self, mock_s3_hook):
        """Test that refresh uses sync for non-versioned bundles."""
        bundle = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            enable_versioning=False,
        )
        
        bundle.refresh()
        
        # Should call sync_to_local_dir
        mock_s3_hook.sync_to_local_dir.assert_called_once_with(
            bucket_name="test-bucket",
            s3_prefix="",
            local_dir=bundle.s3_dags_dir,
            delete_stale=True,
        )

    def test_bundle_path_differs_based_on_versioning(self):
        """Test that bundle paths are different for versioned vs non-versioned bundles."""
        # Non-versioned bundle
        bundle1 = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            enable_versioning=False,
        )
        
        # Versioned bundle
        bundle2 = S3DagBundle(
            name="test",
            bucket_name="test-bucket",
            version="v1.2.3",
            enable_versioning=True,
        )
        
        # Paths should be different
        assert bundle1.s3_dags_dir != bundle2.s3_dags_dir
        assert "v1.2.3" in str(bundle2.s3_dags_dir)
        assert "v1.2.3" not in str(bundle1.s3_dags_dir)

    def test_repr_includes_versioning_info(self):
        """Test that bundle repr includes versioning information."""
        bundle = S3DagBundle(
            name="test",
            bucket_name="my-bucket",
            prefix="dags/",
            version="v1.0.0",
            enable_versioning=True,
        )
        
        repr_str = repr(bundle)
        assert "S3DagBundle" in repr_str
        assert "my-bucket" in repr_str
        assert "dags/" in repr_str
        assert "v1.0.0" in repr_str
        assert "versioning_enabled" in repr_str
