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
import json
import os
from pathlib import Path

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle, BundleVersion
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import AirflowException

MANIFEST_SCHEMA_VERSION = 1


class S3DagBundle(BaseDagBundle):
    """
    S3 Dag bundle - exposes a directory in S3 as a Dag bundle.

    This allows Airflow to load Dags directly from an S3 bucket.
    When S3 bucket versioning is enabled, the bundle generates a manifest
    mapping each object key to its S3 version ID, enabling reproducible
    dag runs and consistent task execution.

    :param aws_conn_id: Airflow connection ID for AWS.  Defaults to AwsBaseHook.default_conn_name.
    :param bucket_name: The name of the S3 bucket containing the Dag files.
    :param prefix:  Optional subdirectory within the S3 bucket where the Dags are stored.
                    If None, Dags are assumed to be at the root of the bucket (Optional).
    """

    supports_versioning = True

    def __init__(
        self,
        *,
        aws_conn_id: str = AwsBaseHook.default_conn_name,
        bucket_name: str,
        prefix: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        # Local path where S3 Dags are downloaded
        self.s3_dags_dir: Path = self.base_dir if not self.version else self.versions_dir / self.version

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            aws_conn_id=self.aws_conn_id,
        )
        self._s3_hook: S3Hook | None = None
        self._cached_bundle_version: BundleVersion | None = None
        self._versioning_warning_logged: bool = False

    def _initialize(self):
        with self.lock():
            if not self.s3_dags_dir.exists():
                self._log.info("Creating local Dags directory: %s", self.s3_dags_dir)
                os.makedirs(self.s3_dags_dir)

            if not self.s3_dags_dir.is_dir():
                raise AirflowException(f"Local Dags path: {self.s3_dags_dir} is not a directory.")

            if not self.s3_hook.check_for_bucket(bucket_name=self.bucket_name):
                raise AirflowException(f"S3 bucket '{self.bucket_name}' does not exist.")

            if self.prefix:
                if not self.s3_hook.check_for_prefix(
                    bucket_name=self.bucket_name, prefix=self.prefix, delimiter="/"
                ):
                    raise AirflowException(
                        f"S3 prefix 's3://{self.bucket_name}/{self.prefix}' does not exist."
                    )

            if self.version and self.version_data:
                # Task execution path: sync specific versions from manifest
                self._sync_versioned(self.version_data)
            else:
                self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def s3_hook(self):
        if self._s3_hook is None:
            try:
                self._s3_hook: S3Hook = S3Hook(aws_conn_id=self.aws_conn_id)
            except AirflowException as e:
                self._log.warning("Could not create S3Hook for connection %s: %s", self.aws_conn_id, e)
        return self._s3_hook

    def __repr__(self):
        return (
            f"<S3DagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def get_current_version(self) -> BundleVersion | None:
        """Return the current version as a BundleVersion with manifest data, or None if S3 versioning is not enabled."""
        if self._cached_bundle_version is not None:
            return self._cached_bundle_version

        result = self._build_manifest()
        if result is None:
            return None

        version_hash, manifest = result
        self._cached_bundle_version = BundleVersion(version=version_hash, data=manifest)
        return self._cached_bundle_version

    def _build_manifest(self) -> tuple[str, dict] | None:
        """
        Call ListObjectVersions, build the manifest, compute the hash.

        Returns (hash, manifest_dict) or None if S3 bucket versioning is not enabled.
        """
        try:
            files: dict[str, str] = {}
            client = self.s3_hook.conn
            paginator = client.get_paginator("list_object_versions")
            page_kwargs: dict = {"Bucket": self.bucket_name}
            if self.prefix:
                page_kwargs["Prefix"] = self.prefix

            for page in paginator.paginate(**page_kwargs):
                for version_entry in page.get("Versions", []):
                    if not version_entry.get("IsLatest"):
                        continue
                    key = version_entry["Key"]
                    version_id = version_entry.get("VersionId")

                    if version_id is None or version_id == "null":
                        # S3 bucket versioning is not enabled
                        if not self._versioning_warning_logged:
                            self._log.warning(
                                "S3 bucket versioning is not enabled on bucket '%s'. "
                                "Falling back to unversioned behavior.",
                                self.bucket_name,
                            )
                            self._versioning_warning_logged = True
                        return None

                    files[key] = version_id

        except Exception:
            self._log.exception(
                "Error listing object versions for bucket '%s'. Falling back to unversioned behavior.",
                self.bucket_name,
            )
            return None

        manifest = {"schema_version": MANIFEST_SCHEMA_VERSION, "files": dict(sorted(files.items()))}
        manifest_bytes = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
        version_hash = hashlib.sha256(manifest_bytes).hexdigest()
        return version_hash, manifest

    def _sync_versioned(self, manifest: dict) -> None:
        """Sync files using specific S3 version IDs from the manifest."""
        schema_version = manifest.get("schema_version")
        if schema_version != MANIFEST_SCHEMA_VERSION:
            raise AirflowException(
                f"Manifest schema version {schema_version} is not supported by this provider version. "
                f"Expected {MANIFEST_SCHEMA_VERSION}. Upgrade the Amazon provider."
            )

        files = manifest.get("files", {})
        client = self.s3_hook.conn

        # Track which local files should exist
        expected_local_files: set[Path] = set()

        for key, version_id in files.items():
            # Compute local path relative to prefix
            if self.prefix and key.startswith(self.prefix):
                rel_key = key[len(self.prefix) :].lstrip("/")
            else:
                rel_key = key

            local_path = self.s3_dags_dir / rel_key
            expected_local_files.add(local_path)

            local_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                response = client.get_object(Bucket=self.bucket_name, Key=key, VersionId=version_id)
                local_path.write_bytes(response["Body"].read())
            except client.exceptions.NoSuchKey:
                raise AirflowException(
                    f"S3 object version '{version_id}' for key '{key}' no longer exists. "
                    f"The bucket's lifecycle policy may have deleted it."
                )
            except Exception:
                self._log.exception(
                    "Error fetching S3 object key='%s' version='%s' from bucket '%s'.",
                    key,
                    version_id,
                    self.bucket_name,
                )
                raise

        # Delete stale local files not in the manifest
        if self.s3_dags_dir.exists():
            for local_file in self.s3_dags_dir.rglob("*"):
                if local_file.is_file() and local_file not in expected_local_files:
                    local_file.unlink()

    @property
    def path(self) -> Path:
        """Return the local path to the Dag files."""
        return self.s3_dags_dir

    def refresh(self) -> None:
        """Refresh the Dag bundle by re-downloading the Dags from S3."""
        # Clear cached version so next get_current_version() rebuilds the manifest
        self._cached_bundle_version = None

        with self.lock():
            self._log.debug(
                "Downloading Dags from s3://%s/%s to %s", self.bucket_name, self.prefix, self.s3_dags_dir
            )
            self.s3_hook.sync_to_local_dir(
                bucket_name=self.bucket_name,
                s3_prefix=self.prefix,
                local_dir=self.s3_dags_dir,
                delete_stale=True,
            )

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the Dags in S3.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use `view_url_template` instead.
        """
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        """Return a URL for viewing the Dags in S3."""
        if hasattr(self, "_view_url_template") and self._view_url_template:
            return self._view_url_template
        url = f"https://{self.bucket_name}.s3"
        if self.s3_hook.region_name:
            url += f".{self.s3_hook.region_name}"
        url += ".amazonaws.com"
        if self.prefix:
            url += f"/{self.prefix}"

        return url
