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

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.models.dag_bundle_version_manifest import DagBundleVersionManifest

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class ManifestSupportingBundle(BaseDagBundle):
    """Test bundle that supports manifest versioning."""
    
    supports_manifest_versioning = True
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mock_manifest_data = {
            "objects": {"test.py": {"etag": "abc123", "version_id": "v1"}},
            "created_at": "2024-01-01T00:00:00Z",
        }
        self.recreate_calls = []
    
    @property
    def path(self) -> Path:
        return Path("/tmp/test")
    
    def get_current_version(self) -> str | None:
        return "test-version-123"
    
    def refresh(self) -> None:
        pass
    
    def create_version_manifest(self) -> dict | None:
        return self.mock_manifest_data.copy()
    
    def recreate_version_from_manifest(self, version_id: str, target_path: Path) -> None:
        self.recreate_calls.append((version_id, target_path))


class NonManifestBundle(BaseDagBundle):
    """Test bundle that does not support manifest versioning."""
    
    supports_manifest_versioning = False
    
    @property
    def path(self) -> Path:
        return Path("/tmp/test")
    
    def get_current_version(self) -> str | None:
        return "simple-version"
    
    def refresh(self) -> None:
        pass


class TestManifestVersioning:
    """Test manifest versioning functionality in BaseDagBundle."""

    @pytest.fixture(autouse=True)
    def bundle_temp_dir(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            yield tmp_path

    # TODO: This test case is really just testing the setting of a class attribute, no functionality, remove it?
    def test_create_version_manifest_not_supported(self):
        """Test create_version_manifest returns None for non-supporting bundles."""
        bundle = NonManifestBundle(name="test_bundle")
        result = bundle.create_version_manifest()
        assert result is None

    def test_create_version_manifest_supported_but_not_implemented(self):
        """Test that bundles claiming support must implement create_version_manifest."""
        
        class IncompleteBundle(BaseDagBundle):
            supports_manifest_versioning = True
            
            @property
            def path(self) -> Path:
                return Path("/tmp/test")
            
            def get_current_version(self) -> str | None:
                return "v1"
            
            def refresh(self) -> None:
                pass
            # Note: missing create_version_manifest implementation

        bundle = IncompleteBundle(name="incomplete")
        with pytest.raises(NotImplementedError, match="does not implement create_version_manifest"):
            bundle.create_version_manifest()

    def test_store_version_manifest(self):
        """Test storing a version manifest."""
        bundle = ManifestSupportingBundle(name="test_bundle")
        version_id = "test-v1"
        manifest_data = {"objects": {"file.py": {"etag": "abc"}}}

        bundle.store_version_manifest(version_id, manifest_data)

        # Verify it was stored in the database
        stored = DagBundleVersionManifest.get_manifest(
            bundle_name="test_bundle",
            version_id=version_id,
        )
        assert stored is not None
        assert stored.manifest_data == manifest_data

    def test_store_version_manifest_not_supported(self):
        """Test storing manifest fails for non-supporting bundles."""
        bundle = NonManifestBundle(name="test_bundle")
        
        with pytest.raises(RuntimeError, match="does not support manifest versioning"):
            bundle.store_version_manifest("v1", {})

    def test_get_version_manifest(self):
        """Test retrieving a version manifest."""
        bundle = ManifestSupportingBundle(name="test_bundle")
        version_id = "test-v2"
        manifest_data = {"objects": {"file2.py": {"etag": "def"}}}

        # Store first
        bundle.store_version_manifest(version_id, manifest_data)

        # Then retrieve
        retrieved_data = bundle.get_version_manifest(version_id)
        assert retrieved_data == manifest_data

    def test_get_version_manifest_nonexistent(self):
        """Test retrieving non-existent manifest returns None."""
        bundle = ManifestSupportingBundle(name="test_bundle")
        
        result = bundle.get_version_manifest("nonexistent-version")
        assert result is None

    def test_get_version_manifest_not_supported(self):
        """Test getting manifest returns None for non-supporting bundles."""
        bundle = NonManifestBundle(name="test_bundle")
        
        result = bundle.get_version_manifest("any-version")
        # TODO: Should this be an exception?
        assert result is None

    def test_recreate_version_from_manifest_not_supported(self):
        """Test recreate fails for non-supporting bundles."""
        bundle = NonManifestBundle(name="test_bundle")
        
        with pytest.raises(RuntimeError, match="does not support manifest versioning"):
            bundle.recreate_version_from_manifest("v1", Path("/tmp"))

    def test_recreate_version_from_manifest_not_implemented(self):
        """Test that bundles claiming support must implement recreate_version_from_manifest."""
        
        class IncompleteBundle(BaseDagBundle):
            supports_manifest_versioning = True
            
            @property
            def path(self) -> Path:
                return Path("/tmp/test")
            
            def get_current_version(self) -> str | None:
                return "v1"
            
            def refresh(self) -> None:
                pass
            
            def create_version_manifest(self) -> dict | None:
                return {}
            # Note: missing recreate_version_from_manifest implementation

        bundle = IncompleteBundle(name="incomplete")
        with pytest.raises(NotImplementedError, match="does not implement recreate_version_from_manifest"):
            bundle.recreate_version_from_manifest("v1", Path("/tmp"))

    def test_end_to_end_manifest_workflow(self):
        """Test the complete manifest workflow: create, store, retrieve, recreate."""
        bundle = ManifestSupportingBundle(name="workflow_test")
        version_id = "workflow-v1"
        target_path = Path("/tmp/recreate-target")

        # 1. Create manifest
        manifest = bundle.create_version_manifest()
        assert manifest is not None
        assert "objects" in manifest

        # 2. Store manifest
        bundle.store_version_manifest(version_id, manifest)

        # 3. Retrieve manifest
        retrieved = bundle.get_version_manifest(version_id)
        assert retrieved == manifest

        # 4. Recreate from manifest
        bundle.recreate_version_from_manifest(version_id, target_path)
        assert bundle.recreate_calls == [(version_id, target_path)]


# TODO: I wonder if this should live with the bundle tests
class TestBundleUsageTrackingManagerManifestCleanup:
    """Test manifest cleanup integration with bundle cleanup."""

    @pytest.fixture(autouse=True)
    def bundle_temp_dir(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            yield tmp_path

    @patch('airflow.dag_processing.bundles.base.create_session')
    def test_stale_bundle_removal_also_removes_manifest(self, mock_create_session):
        """Test that removing stale bundles also removes associated manifests."""
        from airflow.dag_processing.bundles.base import BundleUsageTrackingManager, TrackedBundleVersionInfo
        from pathlib import Path
        from datetime import datetime
        
        # Mock session and manifest
        mock_session = Mock()
        mock_create_session.return_value.__enter__.return_value = mock_session
        
        mock_manifest = Mock()
        mock_session.get.return_value = mock_manifest
        
        # Create bundle version info
        bundle_name = "test_bundle"
        version = "v1.0.0"
        info = TrackedBundleVersionInfo(
            lock_file_path=Path("/tmp/test.lock"),
            version=version,
            dt=datetime.now(),
        )
        
        # Mock filesystem operations
        with (
            patch('airflow.dag_processing.bundles.base.shutil.rmtree') as mock_rmtree,
            patch('airflow.dag_processing.bundles.base.os.remove') as mock_remove,
            patch('builtins.open') as mock_open,
            patch('airflow.dag_processing.bundles.base.flock') as mock_flock,
        ):
            mock_file = Mock()
            mock_open.return_value.__enter__.return_value = mock_file
            
            # Call the method
            BundleUsageTrackingManager._remove_stale_bundle(bundle_name, info)
            
            # Verify filesystem cleanup happened
            mock_rmtree.assert_called_once()
            mock_remove.assert_called_once()
            
            # Verify manifest cleanup was attempted
            mock_session.get.assert_called_once_with(DagBundleVersionManifest, (bundle_name, version))
            mock_session.delete.assert_called_once_with(mock_manifest)
            mock_session.commit.assert_called_once()

    @patch('airflow.dag_processing.bundles.base.create_session')
    def test_stale_bundle_removal_handles_missing_manifest(self, mock_create_session):
        """Test that missing manifests don't break bundle removal."""
        from airflow.dag_processing.bundles.base import BundleUsageTrackingManager, TrackedBundleVersionInfo
        from pathlib import Path
        from datetime import datetime
        
        # Mock session with no manifest found
        mock_session = Mock()
        mock_create_session.return_value.__enter__.return_value = mock_session
        mock_session.get.return_value = None  # No manifest found
        
        bundle_name = "test_bundle"
        version = "v1.0.0"
        info = TrackedBundleVersionInfo(
            lock_file_path=Path("/tmp/test.lock"),
            version=version,
            dt=datetime.now(),
        )
        
        with (
            patch('airflow.dag_processing.bundles.base.shutil.rmtree'),
            patch('airflow.dag_processing.bundles.base.os.remove'),
            patch('builtins.open'),
            patch('airflow.dag_processing.bundles.base.flock'),
        ):
            # Should not raise an exception
            BundleUsageTrackingManager._remove_stale_bundle(bundle_name, info)
            
            # Verify we still attempted to check for manifest
            mock_session.get.assert_called_once()
            # But delete and commit should not be called since no manifest found
            mock_session.delete.assert_not_called()

    @patch('airflow.dag_processing.bundles.base.create_session')
    @patch('airflow.dag_processing.bundles.base.log')
    def test_stale_bundle_removal_handles_manifest_error(self, mock_log, mock_create_session):
        """Test that manifest cleanup errors don't break bundle removal."""
        from airflow.dag_processing.bundles.base import BundleUsageTrackingManager, TrackedBundleVersionInfo
        from pathlib import Path
        from datetime import datetime
        
        # Mock session that raises an error during manifest operations
        mock_session = Mock()
        mock_create_session.return_value.__enter__.return_value = mock_session
        mock_session.get.side_effect = Exception("Database error")
        
        bundle_name = "test_bundle"
        version = "v1.0.0"
        info = TrackedBundleVersionInfo(
            lock_file_path=Path("/tmp/test.lock"),
            version=version,
            dt=datetime.now(),
        )
        
        with (
            patch('airflow.dag_processing.bundles.base.shutil.rmtree') as mock_rmtree,
            patch('airflow.dag_processing.bundles.base.os.remove') as mock_remove,
            patch('builtins.open'),
            patch('airflow.dag_processing.bundles.base.flock'),
        ):
            # Should not raise an exception despite manifest error
            BundleUsageTrackingManager._remove_stale_bundle(bundle_name, info)
            
            # Verify filesystem cleanup still happened
            mock_rmtree.assert_called_once()
            mock_remove.assert_called_once()
            
            # Verify error was logged
            mock_log.warning.assert_called_once()
            warning_call = mock_log.warning.call_args[0]
            assert "failed to remove manifest" in warning_call[0]
            assert bundle_name in warning_call[1]
            assert version in warning_call[2]
