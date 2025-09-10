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
Restructure callback table.

Revision ID: b87d2135fa50
Revises: eaf332f43c7c
Create Date: 2025-09-10 13:58:23.435028

"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy_utils
from alembic import op

import airflow

# revision identifiers, used by Alembic.
revision = "b87d2135fa50"
down_revision = "eaf332f43c7c"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """
    Restructure callback table.

    Add/drop/modify columns, create foreign key for trigger and rename the table name.
    """
    op.rename_table("callback_request", "callback")
    with op.batch_alter_table("callback", schema=None) as batch_op:
        batch_op.add_column(sa.Column("data", airflow.utils.sqlalchemy.ExtendedJSON(), nullable=True))
        batch_op.add_column(sa.Column("state", sa.String(length=10), nullable=True))
        batch_op.add_column(sa.Column("output", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("trigger_id", sa.Integer(), nullable=True))
        batch_op.alter_column(
            "id",
            existing_type=sa.INTEGER(),
            type_=sqlalchemy_utils.types.uuid.UUIDType(binary=False),
            existing_nullable=False,
        )
        batch_op.create_foreign_key(batch_op.f("callback_trigger_id_fkey"), "trigger", ["trigger_id"], ["id"])
        batch_op.drop_column("callback_data")


def downgrade():
    """
    Unapply the callback table restructure.

    Add/drop/modify columns, create foreign key for trigger and rename the table name.
    """
    with op.batch_alter_table("callback", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("callback_data", airflow.utils.sqlalchemy.ExtendedJSON(), nullable=False)
        )
        batch_op.drop_constraint(batch_op.f("callback_trigger_id_fkey"), type_="foreignkey")
        batch_op.alter_column(
            "id",
            existing_type=sqlalchemy_utils.types.uuid.UUIDType(binary=False),
            type_=sa.INTEGER(),
            existing_nullable=False,
        )
        batch_op.drop_column("trigger_id")
        batch_op.drop_column("output")
        batch_op.drop_column("state")
        batch_op.drop_column("data")
    op.rename_table("callback", "callback_request")
