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

from airflow.models.dag_bundle_version_manifest import DagBundleVersionManifest
from airflow.utils.session import create_session

pytestmark = pytest.mark.db_test


class TestDagBundleVersionManifest:
    """Test the DagBundleVersionManifest model."""

    def test_store_and_retrieve_manifest(self):
        """Test storing and retrieving a manifest."""
        bundle_name = "test_bundle"
        version_id = "v1.2.3"
        manifest_data = {
            "bucket": "my-bucket",
            "prefix": "dags/",
            # TODO: Not all fields of the manifest are being tested, but the ones that are being used are.
            "objects": {
                "dag1.py": {"etag": "abc123", "version_id": "v1"},
                "dag2.py": {"etag": "def456", "version_id": "v2"},
            },
        }

        # Store manifest
        manifest = DagBundleVersionManifest.store_manifest(
            bundle_name=bundle_name,
            version_id=version_id,
            manifest_data=manifest_data,
        )

        assert manifest.bundle_name == bundle_name
        assert manifest.version_id == version_id
        assert manifest.manifest_data == manifest_data
        assert manifest.created_at is not None

        # Retrieve manifest
        retrieved = DagBundleVersionManifest.get_manifest(
            bundle_name=bundle_name,
            version_id=version_id,
        )

        assert retrieved is not None
        assert retrieved.bundle_name == bundle_name
        assert retrieved.version_id == version_id
        assert retrieved.manifest_data == manifest_data

    def test_get_nonexistent_manifest(self):
        """Test retrieving a non-existent manifest returns None."""
        result = DagBundleVersionManifest.get_manifest(
            bundle_name="nonexistent",
            version_id="v999",
        )
        assert result is None

    def test_store_duplicate_manifest_uses_merge(self):
        """Test that storing the same manifest twice updates rather than creates duplicate."""
        bundle_name = "test_bundle_dup"
        version_id = "v1.0.0"
        
        # First manifest
        manifest_data_1 = {"version": 1, "objects": {"file1.py": {"etag": "abc"}}}
        manifest1 = DagBundleVersionManifest.store_manifest(
            bundle_name=bundle_name,
            version_id=version_id,
            manifest_data=manifest_data_1,
        )

        # Second manifest with same key but different data
        manifest_data_2 = {"version": 2, "objects": {"file2.py": {"etag": "def"}}}
        manifest2 = DagBundleVersionManifest.store_manifest(
            bundle_name=bundle_name,
            version_id=version_id,
            manifest_data=manifest_data_2,
        )

        # Should have updated the existing record
        retrieved = DagBundleVersionManifest.get_manifest(
            bundle_name=bundle_name,
            version_id=version_id,
        )
        
        assert retrieved.manifest_data == manifest_data_2
        
        # Verify only one record exists
        with create_session() as session:
            count = session.query(DagBundleVersionManifest).filter_by(
                bundle_name=bundle_name, version_id=version_id
            ).count()
            assert count == 1

    def test_get_versions_for_bundle(self):
        """Test getting all versions for a bundle."""
        bundle_name = "multi_version_bundle"
        
        # Create multiple versions
        versions = ["v1.0.0", "v1.1.0", "v1.2.0"]
        for i, version in enumerate(versions):
            DagBundleVersionManifest.store_manifest(
                bundle_name=bundle_name,
                version_id=version,
                manifest_data={"version": i, "objects": {}},
            )

        # Get all versions
        # TODO: Is that method actually used anywhere?
        manifests = DagBundleVersionManifest.get_versions_for_bundle(bundle_name=bundle_name)
        
        assert len(manifests) == 3
        # Should be ordered by created_at descending (newest first)
        manifest_versions = [m.version_id for m in manifests]
        assert manifest_versions == ["v1.2.0", "v1.1.0", "v1.0.0"]

    def test_get_versions_for_nonexistent_bundle(self):
        """Test getting versions for a bundle that doesn't exist."""
        manifests = DagBundleVersionManifest.get_versions_for_bundle(bundle_name="nonexistent")
        assert manifests == []

    def test_cleanup_old_manifests(self):
        """Test cleaning up old manifests while keeping recent ones."""
        bundle_name = "cleanup_test_bundle"
        
        # Create 5 versions
        for i in range(5):
            DagBundleVersionManifest.store_manifest(
                bundle_name=bundle_name,
                version_id=f"v1.{i}.0",
                manifest_data={"version": i, "objects": {}},
            )

        # Cleanup keeping only 2 most recent
        # TODO: Is this used now that we do Manifest cleanup inline with version cache cleanup?
        deleted_count = DagBundleVersionManifest.cleanup_old_manifests(
            bundle_name=bundle_name,
            keep_count=2,
        )

        assert deleted_count == 3

        # Verify only 2 remain
        remaining = DagBundleVersionManifest.get_versions_for_bundle(bundle_name=bundle_name)
        assert len(remaining) == 2
        
        # Should be the 2 most recent
        remaining_versions = [m.version_id for m in remaining]
        assert "v1.4.0" in remaining_versions
        assert "v1.3.0" in remaining_versions

    def test_cleanup_when_fewer_than_keep_count(self):
        """Test cleanup when there are fewer manifests than keep_count."""
        bundle_name = "small_cleanup_bundle"
        
        # Create only 2 versions
        for i in range(2):
            DagBundleVersionManifest.store_manifest(
                bundle_name=bundle_name,
                version_id=f"v2.{i}.0",
                manifest_data={"version": i, "objects": {}},
            )

        # Try to cleanup keeping 5 (more than exist)
        deleted_count = DagBundleVersionManifest.cleanup_old_manifests(
            bundle_name=bundle_name,
            keep_count=5,
        )

        assert deleted_count == 0

        # Verify all remain
        remaining = DagBundleVersionManifest.get_versions_for_bundle(bundle_name=bundle_name)
        assert len(remaining) == 2

    def test_repr(self):
        """Test the string representation of the model."""
        manifest = DagBundleVersionManifest(
            bundle_name="test_bundle",
            version_id="v1.0.0",
            manifest_data={},
        )
        
        repr_str = repr(manifest)
        assert "DagBundleVersionManifest" in repr_str
        assert "test_bundle" in repr_str
        assert "v1.0.0" in repr_str

    def test_composite_primary_key(self):
        """Test that the composite primary key works correctly."""
        # Same version_id but different bundle_name should be allowed
        DagBundleVersionManifest.store_manifest(
            bundle_name="bundle_a",
            version_id="v1.0.0",
            manifest_data={"source": "a"},
        )
        
        DagBundleVersionManifest.store_manifest(
            bundle_name="bundle_b",
            version_id="v1.0.0",  # Same version_id
            manifest_data={"source": "b"},
        )

        # Both should exist
        manifest_a = DagBundleVersionManifest.get_manifest(
            bundle_name="bundle_a", version_id="v1.0.0"
        )
        manifest_b = DagBundleVersionManifest.get_manifest(
            bundle_name="bundle_b", version_id="v1.0.0"
        )

        assert manifest_a is not None
        assert manifest_b is not None
        assert manifest_a.manifest_data["source"] == "a"
        assert manifest_b.manifest_data["source"] == "b"
