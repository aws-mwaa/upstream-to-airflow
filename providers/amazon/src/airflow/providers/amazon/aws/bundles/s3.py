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
import os
import random
import time
from pathlib import Path

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3DagBundle(BaseDagBundle):
    """
    S3 DAG bundle - exposes a directory in S3 as a DAG bundle.

    This allows Airflow to load DAGs directly from an S3 bucket.

    :param aws_conn_id: Airflow connection ID for AWS.  Defaults to AwsBaseHook.default_conn_name.
    :param bucket_name: The name of the S3 bucket containing the DAG files.
    :param prefix:  Optional subdirectory within the S3 bucket where the DAGs are stored.
                    If None, DAGs are assumed to be at the root of the bucket (Optional).
    :param enable_versioning: Enable manifest-based versioning for this S3 bundle.
                             Requires S3 bucket versioning to be enabled (Optional, defaults to False).
    """

    supports_versioning = False
    supports_manifest_versioning = False

    def __init__(
        self,
        *,
        aws_conn_id: str = AwsBaseHook.default_conn_name,
        bucket_name: str,
        prefix: str = "",
        enable_versioning: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.enable_versioning = enable_versioning
        self._s3_hook: S3Hook | None = None
        
        # Set versioning support - defer S3 checks until initialization
        if self.enable_versioning:
            # Provisionally enable manifest versioning - will be validated during initialization
            self.supports_manifest_versioning = True
        
        # Local path where S3 DAGs are downloaded
        if self.version and self.enable_versioning:
            self.s3_dags_dir: Path = self.versions_dir / self.version
        else:
            self.s3_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            aws_conn_id=self.aws_conn_id,
            enable_versioning=self.enable_versioning,
        )

    def _initialize(self):
        with self.lock():
            # Validate versioning support now that S3 hook is available
            if self.enable_versioning:
                self._configure_versioning_support()

            if not self.s3_dags_dir.exists():
                self._log.info("Creating local DAGs directory: %s", self.s3_dags_dir)
                os.makedirs(self.s3_dags_dir)

            if not self.s3_dags_dir.is_dir():
                raise AirflowException(f"Local DAGs path: {self.s3_dags_dir} is not a directory.")

            if not self.s3_hook.check_for_bucket(bucket_name=self.bucket_name):
                raise AirflowException(f"S3 bucket '{self.bucket_name}' does not exist.")

            if self.prefix:
                # don't check when prefix is ""
                if not self.s3_hook.check_for_prefix(
                    bucket_name=self.bucket_name, prefix=self.prefix, delimiter="/"
                ):
                    raise AirflowException(
                        f"S3 prefix 's3://{self.bucket_name}/{self.prefix}' does not exist."
                    )
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def s3_hook(self):
        if self._s3_hook is None:
            try:
                self._s3_hook: S3Hook = S3Hook(aws_conn_id=self.aws_conn_id)  # Initialize S3 hook.
            except AirflowException as e:
                self._log.warning("Could not create S3Hook for connection %s: %s", self.aws_conn_id, e)
        return self._s3_hook

    def _configure_versioning_support(self) -> None:
        """Configure versioning support based on user configuration and S3 bucket capabilities."""
        if not self.enable_versioning:
            return
            
        # Check if S3 bucket has versioning enabled
        if self._bucket_has_versioning():
            self.supports_versioning = True
            self.supports_manifest_versioning = True
            self._log.info("S3 bundle versioning enabled - bucket supports versioning")
        else:
            if self.version:
                # User is trying to use a specific version but bucket doesn't support it
                raise AirflowException(
                    f"S3 bundle versioning requires bucket '{self.bucket_name}' to have versioning enabled. "
                    "Please enable S3 bucket versioning or set enable_versioning=False."
                )
            else:
                # Just warn and fall back to non-versioned behavior
                self._log.warning(
                    "S3 bundle versioning requested but bucket '%s' does not have versioning enabled. "
                    "Falling back to non-versioned behavior.", 
                    self.bucket_name
                )

    def _bucket_has_versioning(self) -> bool:
        """Check if the S3 bucket has versioning enabled."""
        try:
            response = self.s3_hook.get_conn().get_bucket_versioning(Bucket=self.bucket_name)
            return response.get('Status') == 'Enabled'
        except Exception as e:
            self._log.warning("Could not check bucket versioning status: %s", e)
            return False

    def __repr__(self):
        return (
            f"<S3DagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}, "
            f"versioning_enabled={self.supports_versioning!r}"
            f")>"
        )

    def create_version_manifest(self) -> dict | None:
        """Create a manifest describing the current state of the S3 bucket/prefix."""
        if not self.supports_manifest_versioning:
            return None
            
        manifest = self._create_s3_manifest()
        return manifest

    def _create_s3_manifest(self) -> dict:
        """
        Create a consistent S3 manifest with validation to handle concurrent changes.
        
        Uses a snapshot + validation approach to try minimize inconsistencies.
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            self._log.debug("Creating S3 manifest (attempt %d/%d)", attempt + 1, max_retries)
            
            # 1. Fast initial scan
            objects = {}
            snapshot_start = time.time()
            
            for obj_summary in self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix=self.prefix):
                try:
                    head_response = self.s3_hook.conn.head_object(
                        Bucket=self.bucket_name, Key=obj_summary
                    )
                    objects[obj_summary] = {
                        # TODO: Do we need all of this metadata? Or is version alone sufficient?
                        'etag': head_response['ETag'].strip('"'),
                        'last_modified': head_response['LastModified'].isoformat(),
                        'size': head_response['ContentLength'],
                        'version_id': head_response.get('VersionId'),
                    }
                except Exception as e:
                    self._log.warning("Could not get metadata for key %s: %s", obj_summary, e)
                    continue
            
            # 2. Validation pass - check if any objects changed during scan
            if len(objects) > 0:
                # TODO: 10 is quite small, consider increasing the sample size or possibly scan all objects?
                sample_keys = random.sample(list(objects.keys()), min(10, len(objects)))
                validation_failed = False
                
                for key in sample_keys:
                    try:
                        current_head = self.s3_hook.get_conn().head_object(
                            Bucket=self.bucket_name, Key=key
                        )
                        current_etag = current_head['ETag'].strip('"')
                        if current_etag != objects[key]['etag']:
                            self._log.debug("Object %s changed during manifest creation", key)
                            validation_failed = True
                            break
                    except Exception as e:
                        self._log.warning("Validation failed for key %s: %s", key, e)
                        validation_failed = True
                        break
                
                if not validation_failed:
                    return {
                        'bucket': self.bucket_name,
                        'prefix': self.prefix,
                        'objects': objects,
                        'snapshot_time': time.time(),
                        'manifest_version': '1.0'
                    }
            
            # Retry with exponential backoff
            if attempt < max_retries - 1:
                sleep_time = (2 ** attempt) + random.uniform(0, 1)
                self._log.debug("Retrying manifest creation in %.2f seconds", sleep_time)
                time.sleep(sleep_time)
        
        raise AirflowException(
            f"Could not create consistent S3 manifest after {max_retries} attempts. "
            "S3 bucket state may be changing too frequently during manifest creation."
        )

    def get_current_version(self) -> str | None:
        """Return a version ID based on the current S3 state."""
        if not self.supports_versioning:
            return None
            
        manifest = self.create_version_manifest()
        if not manifest:
            return None
            
        # Create deterministic version ID from manifest
        manifest_str = str(sorted(manifest['objects'].items()))
        version_hash = hashlib.sha256(manifest_str.encode()).hexdigest()
        return f"s3-{version_hash[:16]}"

    @property
    def path(self) -> Path:
        """Return the local path to the DAG files."""
        return self.s3_dags_dir  # Path where DAGs are downloaded.

    def refresh(self) -> None:
        """Refresh the DAG bundle by re-downloading the DAGs from S3."""
        if self.version and self.supports_versioning:
            # When working with a specific version, recreate from manifest
            self.recreate_version_from_manifest(self.version, self.s3_dags_dir)
        else:
            # Regular refresh from current S3 state
            with self.lock():
                self._log.debug(
                    "Downloading DAGs from s3://%s/%s to %s", self.bucket_name, self.prefix, self.s3_dags_dir
                )
                self.s3_hook.sync_to_local_dir(
                    bucket_name=self.bucket_name,
                    s3_prefix=self.prefix,
                    local_dir=self.s3_dags_dir,
                    delete_stale=True,
                )

    def recreate_version_from_manifest(self, version_id: str, target_path: Path) -> None:
        """
        Recreate a specific version of the bundle from its stored manifest.

        Downloads the exact S3 object versions specified in the manifest to recreate
        the bundle state at the time the manifest was created.

        :param version_id: The version ID to recreate
        :param target_path: The local path where the version should be recreated
        """
        if not self.supports_manifest_versioning:
            raise RuntimeError("Bundle does not support manifest versioning")

        manifest_data = self.get_version_manifest(version_id)
        if not manifest_data:
            raise AirflowException(f"No manifest found for version {version_id}")

        with self.lock():
            self._log.info("Recreating S3 bundle version %s to %s", version_id, target_path)
            
            # Ensure target directory exists and is clean
            target_path.mkdir(parents=True, exist_ok=True)
            
            # Download each object with its specific version
            for object_key, object_info in manifest_data['objects'].items():
                local_file_path = target_path / Path(object_key).relative_to(Path(self.prefix) if self.prefix else Path('.'))
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                try:
                    # Use version_id if available
                    download_kwargs = {'Bucket': self.bucket_name, 'Key': object_key}
                    # TODO: add else case that raises if version_id is not available
                    if object_info.get('version_id'):
                        download_kwargs['VersionId'] = object_info['version_id']
                    
                    self._log.debug("Downloading %s (version: %s) to %s", 
                                  object_key, object_info.get('version_id', 'latest'), local_file_path)
                    
                    self.s3_hook.conn.download_file(
                        Filename=str(local_file_path),
                        **download_kwargs
                    )
                    
                except Exception as e:
                    self._log.error("Failed to download %s: %s", object_key, e)
                    raise AirflowException(f"Failed to recreate version {version_id}: could not download {object_key}")
            
            self._log.info("Successfully recreated S3 bundle version %s", version_id)

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the DAGs in S3. Currently, versioning is not supported.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use `view_url_template` instead.
        """
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        """Return a URL for viewing the DAGs in S3. Currently, versioning is not supported."""
        if self.version:
            raise AirflowException("S3 url with version is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            # Because we use this method in the view_url method, we need to handle
            # backward compatibility for Airflow versions that doesn't have the
            # _view_url_template attribute. Should be removed when we drop support for Airflow 3.0
            return self._view_url_template
        # https://<bucket-name>.s3.<region>.amazonaws.com/<object-key>
        url = f"https://{self.bucket_name}.s3"
        if self.s3_hook.region_name:
            url += f".{self.s3_hook.region_name}"
        url += ".amazonaws.com"
        if self.prefix:
            url += f"/{self.prefix}"

        return url
