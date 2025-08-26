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

from sqlalchemy import Column, select
from sqlalchemy.orm import relationship

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import JSON, UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class DagBundleVersionManifest(Base, LoggingMixin):
    """
    A table for storing version manifests for DAG bundles.

    This table stores detailed manifest data that defines the exact state
    of a bundle at a specific version. This enables bundle types that don't
    have native atomic versioning (like bucket/block storage) to implement
    versioning by capturing and storing complete state snapshots.

    The manifest_data field contains bundle-specific json serialized information
    needed to recreate that exact version of the bundle later.
    """

    __tablename__ = "dag_bundle_version_manifest"

    bundle_name = Column(StringID(), primary_key=True, nullable=False)
    version_id = Column(StringID(), primary_key=True, nullable=False)
    manifest_data = Column(JSON, nullable=False)
    created_at = Column(UtcDateTime, nullable=False, default=timezone.utcnow)

    def __init__(self, *, bundle_name: str, version_id: str, manifest_data: dict):
        super().__init__()
        self.bundle_name = bundle_name
        self.version_id = version_id
        self.manifest_data = manifest_data

    def __repr__(self):
        return f"<DagBundleVersionManifest(bundle_name='{self.bundle_name}', version_id='{self.version_id}')>"

    @classmethod
    @provide_session
    def store_manifest(
        cls,
        *,
        bundle_name: str,
        version_id: str,
        manifest_data: dict,
        session: Session = NEW_SESSION,
    ) -> DagBundleVersionManifest:
        """
        Store a version manifest in the database.

        :param bundle_name: The bundle name
        :param version_id: The version identifier  
        :param manifest_data: The manifest data as a dictionary
        :param session: Database session
        :return: The created manifest record
        """
        manifest = cls(
            bundle_name=bundle_name,
            version_id=version_id,
            manifest_data=manifest_data,
        )
        session.merge(manifest)  # Use merge to handle duplicates gracefully
        session.commit()
        return manifest

    @classmethod
    @provide_session
    def get_manifest(
        cls,
        *,
        bundle_name: str,
        version_id: str,
        session: Session = NEW_SESSION,
    ) -> DagBundleVersionManifest | None:
        """
        Retrieve a version manifest from the database.

        :param bundle_name: The bundle name
        :param version_id: The version identifier
        :param session: Database session
        :return: The manifest record or None if not found
        """
        return session.get(cls, (bundle_name, version_id))

    @classmethod
    @provide_session
    def get_versions_for_bundle(
        cls,
        *,
        bundle_name: str,
        session: Session = NEW_SESSION,
    ) -> list[DagBundleVersionManifest]:
        """
        Get all version manifests for a given bundle.

        :param bundle_name: The bundle name
        :param session: Database session
        :return: List of manifest records ordered by creation time (newest first)
        """
        return list(
            session.scalars(
                select(cls)
                .where(cls.bundle_name == bundle_name)
                .order_by(cls.created_at.desc())
            )
        )

    @classmethod
    @provide_session
    def cleanup_old_manifests(
        cls,
        *,
        bundle_name: str,
        keep_count: int = 10,
        session: Session = NEW_SESSION,
    ) -> int:
        """
        Clean up old manifests for a bundle, keeping only the most recent ones.

        :param bundle_name: The bundle name
        :param keep_count: Number of recent manifests to keep
        :param session: Database session
        :return: Number of manifests deleted
        """
        # Get manifests ordered by creation time (oldest first for deletion)
        old_manifests = list(
            session.scalars(
                select(cls)
                .where(cls.bundle_name == bundle_name)
                .order_by(cls.created_at.asc())
                .limit(max(0, session.scalar(
                    select(cls).where(cls.bundle_name == bundle_name).count()
                ) - keep_count))
            )
        )
        
        deleted_count = len(old_manifests)
        for manifest in old_manifests:
            session.delete(manifest)
        
        session.commit()
        return deleted_count
