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

"""Add dag_bundle_version_manifest table

Revision ID: 0083_3_1_0_add_dag_bundle_version_manifest
Revises: 0082_3_1_0_make_bundle_name_not_nullable
Create Date: 2025-08-22 15:38:00.000000

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.models.base import StringID
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "0083_3_1_0_add_dag_bundle_version_manifest"
down_revision = "0082_3_1_0_make_bundle_name_not_nullable"
branch_labels = None
depends_on = None


def upgrade():
    """Apply Add dag_bundle_version_manifest table."""
    op.create_table(
        "dag_bundle_version_manifest",
        sa.Column("bundle_name", StringID(), nullable=False),
        sa.Column("version_id", StringID(), nullable=False),
        sa.Column("manifest_data", sa.JSON(), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("bundle_name", "version_id", name=op.f("dag_bundle_version_manifest_pkey")),
    )


def downgrade():
    """Unapply Add dag_bundle_version_manifest table."""
    op.drop_table("dag_bundle_version_manifest")
