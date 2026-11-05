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

"""
Add per-run scheduling overrides to dag_run and an override opt-in to slot_pool.

Revision ID: a117c0de5e01
Revises: 16d7301f124b
Create Date: 2026-11-05 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a117c0de5e01"
down_revision = "16d7301f124b"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add dag_run.scheduling_overrides, dag_run.has_scheduling_overrides and slot_pool.allow_overrides."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "scheduling_overrides",
                sa.JSON().with_variant(postgresql.JSONB(), "postgresql"),
                nullable=True,
            )
        )
        batch_op.add_column(sa.Column("has_scheduling_overrides", sa.Boolean(), nullable=True))
    with op.batch_alter_table("slot_pool", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("allow_overrides", sa.Boolean(), nullable=False, server_default=sa.false())
        )


def downgrade():
    """Drop the per-run scheduling override columns."""
    with op.batch_alter_table("slot_pool", schema=None) as batch_op:
        batch_op.drop_column("allow_overrides")
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("has_scheduling_overrides")
        batch_op.drop_column("scheduling_overrides")
